#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia operações de dados de estoques de carbono no banco de dados Oracle.

Este módulo implementa operações CRUD para dados de estoques de carbono,
incluindo solo, biomassa e matéria orgânica morta, seguindo o protocolo
GHG para o setor agrícola e considerando a amortização de mudanças.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List

import cx_Oracle

from persistence.oracle.connector import OracleConnector
from persistence.oracle.error_handler import with_error_handling, with_retry
from persistence.oracle.session_dao import SessionDAO

# Configuração de logging
logger = logging.getLogger(__name__)


class CarbonStockDAO:
    """
    Implementa operações de persistência para dados de estoques de carbono.

    Fornece métodos para salvar, recuperar e analisar dados de estoques
    de carbono em diferentes reservatórios (solo, biomassa, matéria orgânica
    morta) e calcular fluxos de CO2 considerando amortização.
    """

    # Constantes para validação
    VALID_STOCK_TYPES = [
        'soil_organic_carbon',
        'above_ground_biomass',
        'below_ground_biomass',
        'dead_organic_matter'
    ]

    VALID_MEASUREMENT_METHODS = [
        'direct_sampling',
        'remote_sensing',
        'model_estimate',
        'default_factor'
    ]

    def __init__(self, connector: OracleConnector, session_dao: SessionDAO = None):
        """
        Inicializa DAO com conector Oracle.

        Args:
            connector: Conector Oracle já inicializado
            session_dao: DAO de sessões para validação (opcional)
        """
        self.connector = connector
        self.session_dao = session_dao

        # Queries SQL para operações comuns
        self._queries = {
            'insert': """
                INSERT INTO carbon_stocks (
                    session_id, timestamp, stock_type, change,
                    amortization_period, unit, measurement_method
                ) VALUES (
                    :session_id, :timestamp, :stock_type, :change,
                    :amortization_period, :unit, :measurement_method
                )
            """,
            'get_by_id': """
                SELECT
                    id, session_id, timestamp, stock_type,
                    change, amortization_period, unit, measurement_method
                FROM carbon_stocks
                WHERE id = :id
            """,
            'get_by_session': """
                SELECT
                    id, timestamp, stock_type, change,
                    amortization_period, unit, measurement_method
                FROM carbon_stocks
                WHERE session_id = :session_id
                ORDER BY timestamp, stock_type
            """,
            'get_by_session_and_type': """
                SELECT
                    id, timestamp, change, amortization_period,
                    unit, measurement_method
                FROM carbon_stocks
                WHERE session_id = :session_id
                  AND stock_type = :stock_type
                ORDER BY timestamp
            """,
            'get_total_by_type': """
                SELECT
                    stock_type,
                    SUM(change) as total_change
                FROM carbon_stocks
                WHERE session_id = :session_id
                GROUP BY stock_type
            """,
            'get_by_time_range': """
                SELECT
                    id, stock_type, change, amortization_period,
                    unit, measurement_method
                FROM carbon_stocks
                WHERE session_id = :session_id
                  AND timestamp BETWEEN :start_time AND :end_time
                ORDER BY timestamp, stock_type
            """,
            'get_luc_data': """
                SELECT
                    id, timestamp, stock_type, change,
                    amortization_period, measurement_method
                FROM carbon_stocks
                WHERE session_id = :session_id
                  AND change < 0  -- Emissões de LUC são negativas
                  AND amortization_period > 0
                ORDER BY timestamp, stock_type
            """,
            'get_sequestration_data': """
                SELECT
                    id, timestamp, stock_type, change,
                    amortization_period, measurement_method
                FROM carbon_stocks
                WHERE session_id = :session_id
                  AND change > 0  -- Sequestro é positivo
                ORDER BY timestamp, stock_type
            """
        }

    @with_error_handling
    @with_retry()
    def save_carbon_stock_data(self, session_id: str,
                             carbon_data: Dict[str, Any]) -> int:
        """
        Salva dados de estoques de carbono para uma sessão específica.

        Estrutura esperada para carbon_data:
        {
            "soil_organic_carbon": {
                "change_co2": 150.5,  # kg CO2
                "amortization_period": 20,  # anos
                "measurement_method": "model_estimate"
            },
            "above_ground_biomass": {
                "change_co2": -75.2,  # kg CO2 (negativo = emissão)
                "amortization_period": 1,  # sem amortização
                "measurement_method": "direct_sampling"
            }
        }

        Args:
            session_id: Identificador da sessão
            carbon_data: Dicionário com dados de estoques de carbono

        Returns:
            int: Número de registros salvos

        Raises:
            ValueError: Se dados ou sessão forem inválidos
            RuntimeError: Se ocorrer erro ao salvar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        # Valida sessão se session_dao estiver disponível
        if self.session_dao and not self.session_dao.validate_session(session_id):
            raise ValueError(f"Sessão {session_id} inválida ou não está ativa")

        if not carbon_data:
            logger.warning("Nenhum dado de estoque de carbono fornecido para salvar")
            return 0

        # Prepara dados para inserção em lote
        batch_data = []
        now = datetime.now()

        # Processa cada tipo de estoque
        for stock_type, stock_data in carbon_data.items():
            # Valida tipo de estoque
            if stock_type not in self.VALID_STOCK_TYPES:
                logger.warning(f"Tipo de estoque inválido: {stock_type}. Ignorando.")
                continue

            if not isinstance(stock_data, dict):
                logger.warning(f"Dados inválidos para estoque {stock_type}. Ignorando.")
                continue

            # Extrai e valida dados
            change_co2 = stock_data.get('change_co2')
            if change_co2 is None:
                logger.warning(f"Valor de mudança não fornecido para {stock_type}. Ignorando.")
                continue

            # Valida valor numérico
            if not isinstance(change_co2, (int, float)):
                logger.warning(f"Valor inválido para {stock_type}: {change_co2}. Ignorando.")
                continue

            # Extrai período de amortização (default: 20 anos)
            amortization_period = stock_data.get('amortization_period', 20)

            # Para emissões de biomassa ou matéria orgânica morta por queima,
            # não usar amortização
            if (stock_type in ['above_ground_biomass', 'dead_organic_matter'] and
                change_co2 < 0 and
                stock_data.get('cause') == 'combustion'):
                amortization_period = 1

            # Período de amortização deve ser inteiro positivo
            try:
                amortization_period = int(amortization_period)
                if amortization_period < 1:
                    amortization_period = 1
            except (ValueError, TypeError):
                amortization_period = 20  # Valor padrão

            # Método de medição (default: model_estimate)
            measurement_method = stock_data.get('measurement_method', 'model_estimate')
            if measurement_method not in self.VALID_MEASUREMENT_METHODS:
                measurement_method = 'model_estimate'

            batch_data.append({
                'session_id': session_id,
                'timestamp': now,
                'stock_type': stock_type,
                'change': change_co2,
                'amortization_period': amortization_period,
                'unit': 'kg CO2',
                'measurement_method': measurement_method
            })

        # Executa inserção em lote se houver dados
        if not batch_data:
            logger.warning("Nenhum dado válido de estoque de carbono para salvar")
            return 0

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Executa inserção em lote
                cursor.executemany(self._queries['insert'], batch_data)

                conn.commit()
                records_saved = len(batch_data)
                logger.info(f"Salvos {records_saved} registros de estoques de carbono "
                          f"para sessão {session_id}")
                return records_saved

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de estoques: {error_obj.message}")
            raise RuntimeError(f"Falha ao salvar dados: {error_obj.message}") from e

    @with_error_handling
    def get_carbon_stock_data_by_session(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Recupera todos os dados de estoques de carbono para uma sessão.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Lista de registros de estoques de carbono

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_session'],
                    session_id=session_id
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar estoques da sessão {session_id}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_carbon_stock_by_type(self, session_id: str,
                               stock_type: str) -> List[Dict[str, Any]]:
        """
        Recupera dados de um tipo específico de estoque de carbono.

        Args:
            session_id: Identificador da sessão
            stock_type: Tipo de estoque de carbono

        Returns:
            List: Lista de registros do estoque

        Raises:
            ValueError: Se tipo de estoque for inválido
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if stock_type not in self.VALID_STOCK_TYPES:
            raise ValueError(f"Tipo de estoque inválido: {stock_type}. "
                           f"Use um dos: {self.VALID_STOCK_TYPES}")

        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_session_and_type'],
                    session_id=session_id,
                    stock_type=stock_type
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar estoque {stock_type}: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_total_carbon_stock_changes(self, session_id: str) -> Dict[str, float]:
        """
        Calcula totais de mudanças em estoques de carbono por tipo.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Totais de mudanças por tipo de estoque

        Raises:
            RuntimeError: Se ocorrer erro ao calcular totais
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_total_by_type'],
                    session_id=session_id
                )

                rows = cursor.fetchall()
                if not rows:
                    return {stock_type: 0.0 for stock_type in self.VALID_STOCK_TYPES}

                # Converte resultado para dicionário
                result = {stock_type: 0.0 for stock_type in self.VALID_STOCK_TYPES}
                for stock_type, total_change in rows:
                    result[stock_type] = total_change

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao calcular totais de estoques: {error_obj.message}")
            raise RuntimeError(f"Falha ao calcular totais: {error_obj.message}") from e

    @with_error_handling
    def get_carbon_stock_structure(self, session_id: str) -> Dict[str, Any]:
        """
        Recupera estrutura de estoques de carbono organizada por categoria.

        Organiza dados nos formatos esperados pelo protocolo GHG,
        separando em LUC, sequestro e emissões de manejo de terra.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Estrutura organizada de estoques de carbono

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            # Recupera dados completos
            stocks_data = self.get_carbon_stock_data_by_session(session_id)

            # Inicializa estrutura de resultado
            result = {
                'land_use_management': {},
                'sequestration_luc': {},
                'biofuel_combustion': {}
            }

            # Processa cada registro
            for stock in stocks_data:
                stock_type = stock['stock_type']
                change = stock['change']

                # Determina categoria apropriada
                if change < 0 and stock.get('amortization_period', 1) > 1:
                    # Emissões LUC (negativos e amortizados)
                    category = 'scope1'  # LUC vai para scope 1
                    if category not in result:
                        result[category] = {'luc': {}}

                    if stock_type not in result[category]['luc']:
                        result[category]['luc'][stock_type] = {'CO2': 0}

                    result[category]['luc'][stock_type]['CO2'] += change

                elif change > 0 and stock.get('amortization_period', 1) > 1:
                    # Sequestro LUC (positivos e amortizados)
                    category = 'sequestration_luc'
                    if stock_type not in result[category]:
                        result[category][stock_type] = {'change_co2': 0}

                    result[category][stock_type]['change_co2'] += change

                elif stock_type in ['above_ground_biomass', 'dead_organic_matter'] and change < 0:
                    # Possível combustão de biomassa
                    if stock.get('measurement_method') == 'combustion':
                        category = 'biofuel_combustion'
                        if stock_type not in result[category]:
                            result[category][stock_type] = {'change_co2': 0}

                        result[category][stock_type]['change_co2'] += change
                    else:
                        # Outro tipo de emissão de manejo
                        category = 'land_use_management'
                        if stock_type not in result[category]:
                            result[category][stock_type] = {'change_co2': 0}

                        result[category][stock_type]['change_co2'] += change

                else:
                    # Fluxos de manejo de terra
                    category = 'land_use_management'
                    if stock_type not in result[category]:
                        result[category][stock_type] = {'change_co2': 0}

                    result[category][stock_type]['change_co2'] += change

            return result

        except Exception as e:
            logger.error(f"Erro ao estruturar dados de estoques: {str(e)}")
            raise RuntimeError(f"Falha ao estruturar dados: {str(e)}") from e

    @with_error_handling
    def get_amortized_flux(self, session_id: str, period: int = 1) -> Dict[str, float]:
        """
        Calcula fluxo amortizado para o período especificado.

        Distribui mudanças em estoques de carbono ao longo do período
        de amortização conforme protocolo GHG.

        Args:
            session_id: Identificador da sessão
            period: Período específico dentro do período total

        Returns:
            Dict: Fluxo amortizado por tipo de estoque

        Raises:
            RuntimeError: Se ocorrer erro ao calcular fluxo
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        if period < 1:
            raise ValueError("Período deve ser um inteiro positivo")

        try:
            # Recupera dados completos
            stocks_data = self.get_carbon_stock_data_by_session(session_id)

            # Inicializa resultado
            result = {stock_type: 0.0 for stock_type in self.VALID_STOCK_TYPES}

            # Processa cada registro
            for stock in stocks_data:
                stock_type = stock['stock_type']
                change = stock['change']
                amort_period = stock.get('amortization_period', 1)

                # Verifica se período solicitado está dentro do período de amortização
                if period <= amort_period:
                    # Distribuição linear
                    if amort_period > 0:
                        amortized_value = change / amort_period
                    else:
                        amortized_value = change

                    result[stock_type] += amortized_value

            return result

        except Exception as e:
            logger.error(f"Erro ao calcular fluxo amortizado: {str(e)}")
            raise RuntimeError(f"Falha ao calcular fluxo: {str(e)}") from e

    @with_error_handling
    def is_land_use_change(self, session_id: str, previous_use: str,
                         current_use: str) -> bool:
        """
        Determina se ocorreu mudança no uso da terra.

        Args:
            session_id: Identificador da sessão
            previous_use: Uso anterior da terra
            current_use: Uso atual da terra

        Returns:
            bool: True se houve mudança de uso da terra
        """
        # Define categorias de uso da terra
        land_use_categories = [
            'forest', 'cropland', 'grassland', 'wetland', 'settlements'
        ]

        # Considerado LUC se a categoria mudou
        return (previous_use in land_use_categories and
                current_use in land_use_categories and
                previous_use != current_use)

    @with_error_handling
    def calculate_historical_impact(self, session_id: str,
                                  historical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula impacto histórico de mudanças em estoques de carbono.

        Considera mudanças que ocorreram antes do período base
        conforme protocolo GHG.

        Args:
            session_id: Identificador da sessão
            historical_data: Dados históricos de uso da terra e práticas

        Returns:
            Dict: Impacto histórico por tipo de estoque

        Raises:
            RuntimeError: Se ocorrer erro ao calcular impacto
        """
        if not historical_data:
            return {}

        result = {stock_type: 0.0 for stock_type in self.VALID_STOCK_TYPES}

        # Processa dados históricos para cada tipo de estoque
        for stock_type, history in historical_data.items():
            if stock_type not in self.VALID_STOCK_TYPES:
                continue

            if not isinstance(history, dict):
                continue

            # Extrai dados históricos
            change = history.get('change_co2', 0)
            start_year = history.get('start_year')
            years_ago = history.get('years_ago')
            amortization_period = history.get('amortization_period', 20)

            # Valida anos
            current_year = datetime.now().year
            if start_year:
                years_passed = current_year - start_year
            elif years_ago:
                years_passed = years_ago
            else:
                years_passed = 0

            # Verifica se ainda está dentro do período de amortização
            if years_passed < amortization_period:
                # Calcula quanto do período de amortização já passou
                remaining_years = amortization_period - years_passed

                # Calcula quanto da mudança ainda precisa ser amortizada
                remaining_change = (change / amortization_period) * remaining_years

                result[stock_type] += remaining_change

        return result
