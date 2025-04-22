#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia operações de dados de emissões GHG no banco de dados Oracle.

Este módulo implementa operações CRUD para dados de emissões de gases de
efeito estufa, classificados por escopo, categoria e fonte, conforme o
protocolo GHG para o setor agrícola.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Union, Tuple

import cx_Oracle

from persistence.oracle.connector import OracleConnector
from persistence.oracle.error_handler import with_error_handling, with_retry
from persistence.oracle.session_dao import SessionDAO

# Configuração de logging
logger = logging.getLogger(__name__)


class EmissionsDAO:
    """
    Implementa operações de persistência para dados de emissões GHG.

    Fornece métodos para salvar, recuperar e analisar dados de emissões
    seguindo o protocolo GHG para o setor agrícola, categorizados por
    escopo, tipo de gás e fonte.
    """

    # Constantes para validação
    VALID_SCOPES = [1, 2, 3]
    VALID_GASES = ['CO2', 'CH4', 'N2O', 'CO2e']
    VALID_CALCULATION_METHODS = ['tier1', 'tier2', 'tier3', 'direct_measurement']

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
                INSERT INTO ghg_emissions (
                    session_id, timestamp, scope, category, source,
                    gas, value, unit, calculation_method, uncertainty_percent
                ) VALUES (
                    :session_id, :timestamp, :scope, :category, :source,
                    :gas, :value, :unit, :calculation_method, :uncertainty_percent
                )
            """,
            'get_by_id': """
                SELECT
                    id, session_id, timestamp, scope, category,
                    source, gas, value, unit, calculation_method,
                    uncertainty_percent
                FROM ghg_emissions
                WHERE id = :id
            """,
            'get_by_session': """
                SELECT
                    id, timestamp, scope, category, source,
                    gas, value, unit, calculation_method, uncertainty_percent
                FROM ghg_emissions
                WHERE session_id = :session_id
                ORDER BY timestamp, scope, category, source, gas
            """,
            'get_by_session_and_scope': """
                SELECT
                    id, timestamp, category, source,
                    gas, value, unit, calculation_method, uncertainty_percent
                FROM ghg_emissions
                WHERE session_id = :session_id
                  AND scope = :scope
                ORDER BY timestamp, category, source, gas
            """,
            'get_totals_by_scope': """
                SELECT
                    scope,
                    SUM(CASE WHEN gas = 'CO2e' THEN value ELSE 0 END) as total_co2e,
                    SUM(CASE WHEN gas = 'CO2' THEN value ELSE 0 END) as total_co2,
                    SUM(CASE WHEN gas = 'CH4' THEN value ELSE 0 END) as total_ch4,
                    SUM(CASE WHEN gas = 'N2O' THEN value ELSE 0 END) as total_n2o
                FROM ghg_emissions
                WHERE session_id = :session_id
                GROUP BY scope
                ORDER BY scope
            """,
            'get_totals_by_category': """
                SELECT
                    scope,
                    category,
                    SUM(CASE WHEN gas = 'CO2e' THEN value ELSE 0 END) as total_co2e
                FROM ghg_emissions
                WHERE session_id = :session_id
                GROUP BY scope, category
                ORDER BY scope, category
            """,
            'get_by_time_range': """
                SELECT
                    id, scope, category, source,
                    gas, value, unit, calculation_method
                FROM ghg_emissions
                WHERE session_id = :session_id
                  AND timestamp BETWEEN :start_time AND :end_time
                ORDER BY timestamp, scope, category, source, gas
            """
        }

    @with_error_handling
    @with_retry()
    def save_emissions_data(self, session_id: str,
                          emissions_data: Dict[str, Any]) -> int:
        """
        Salva dados de emissões GHG para uma sessão específica.

        Estrutura esperada para emissions_data:
        {
            "scope1": {
                "mechanical": {
                    "fuel_combustion": {
                        "CO2": 100.5,
                        "CH4": 0.2,
                        "N2O": 0.01,
                        "CO2e": 105.3
                    }
                },
                "non_mechanical": {...},
                "luc": {...}
            },
            "scope2": {...},
            "scope3": {...}
        }

        Args:
            session_id: Identificador da sessão
            emissions_data: Dicionário estruturado com dados de emissões

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

        if not emissions_data:
            logger.warning("Nenhum dado de emissão fornecido para salvar")
            return 0

        # Prepara dados para inserção em lote
        batch_data = []
        now = datetime.now()

        # Processa cada escopo de emissões
        for scope_name, scope_data in emissions_data.items():
            if not isinstance(scope_data, dict):
                logger.warning(f"Dados inválidos para escopo {scope_name}. Ignorando.")
                continue

            # Extrai número do escopo (scope1 -> 1)
            try:
                scope_num = int(scope_name.replace('scope', ''))
                if scope_num not in self.VALID_SCOPES:
                    logger.warning(f"Escopo inválido: {scope_num}. Ignorando.")
                    continue
            except ValueError:
                logger.warning(f"Nome de escopo inválido: {scope_name}. Ignorando.")
                continue

            # Escopo 1 tem categorias específicas
            if scope_num == 1:
                for category, category_data in scope_data.items():
                    if not isinstance(category_data, dict):
                        continue

                    for source, source_data in category_data.items():
                        if not isinstance(source_data, dict):
                            continue

                        # Processa cada gás para esta fonte
                        batch_data.extend(
                            self._prepare_emission_records(
                                session_id, now, scope_num, category,
                                source, source_data
                            )
                        )
            else:
                # Escopos 2 e 3 são mais simples (sem categorias)
                for source, source_data in scope_data.items():
                    if not isinstance(source_data, dict):
                        continue

                    # Processa cada gás para esta fonte
                    batch_data.extend(
                        self._prepare_emission_records(
                            session_id, now, scope_num, '', source, source_data
                        )
                    )

        # Executa inserção em lote se houver dados
        if not batch_data:
            logger.warning("Nenhum dado válido de emissão para salvar")
            return 0

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Executa inserção em lote
                cursor.executemany(self._queries['insert'], batch_data)

                conn.commit()
                records_saved = len(batch_data)
                logger.info(f"Salvos {records_saved} registros de emissões GHG "
                          f"para sessão {session_id}")
                return records_saved

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de emissões: {error_obj.message}")
            raise RuntimeError(f"Falha ao salvar dados: {error_obj.message}") from e

    def _prepare_emission_records(self, session_id: str, timestamp: datetime,
                                scope: int, category: str, source: str,
                                gas_data: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Prepara registros de emissão para inserção em lote.

        Método auxiliar para formatar dados e aplicar validações.

        Args:
            session_id: Identificador da sessão
            timestamp: Timestamp da emissão
            scope: Número do escopo (1, 2 ou 3)
            category: Categoria da emissão (para escopo 1)
            source: Fonte da emissão
            gas_data: Dados de emissão por tipo de gás

        Returns:
            List: Lista de registros formatados para inserção
        """
        records = []

        for gas, value in gas_data.items():
            # Pula entradas que não são gases
            if gas not in self.VALID_GASES:
                continue

            # Valida valor da emissão
            if not isinstance(value, (int, float)):
                logger.warning(
                    f"Valor inválido para gás {gas} na fonte {source}: {value}. "
                    f"Ignorando."
                )
                continue

            # Valores negativos só são permitidos para CO2 em sumidouros de carbono
            if value < 0 and gas != 'CO2':
                logger.warning(
                    f"Valor negativo não permitido para gás {gas}: {value}. "
                    f"Ignorando."
                )
                continue

            records.append({
                'session_id': session_id,
                'timestamp': timestamp,
                'scope': scope,
                'category': category,
                'source': source,
                'gas': gas,
                'value': value,
                'unit': 'kg',  # Unidade padrão para emissões
                'calculation_method': 'tier1',  # Método padrão
                'uncertainty_percent': 10.0  # Valor padrão de incerteza
            })

        return records

    @with_error_handling
    def get_emissions_by_session(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Recupera todos os dados de emissões para uma sessão.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Lista de registros de emissões

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
            logger.error(f"Erro ao consultar emissões da sessão {session_id}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_emissions_by_scope(self, session_id: str,
                              scope: int) -> List[Dict[str, Any]]:
        """
        Recupera emissões de um escopo específico.

        Args:
            session_id: Identificador da sessão
            scope: Número do escopo (1, 2 ou 3)

        Returns:
            List: Lista de emissões do escopo

        Raises:
            ValueError: Se escopo for inválido
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if scope not in self.VALID_SCOPES:
            raise ValueError(f"Escopo inválido: {scope}. Use um dos: {self.VALID_SCOPES}")

        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_session_and_scope'],
                    session_id=session_id,
                    scope=scope
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar emissões do escopo {scope}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_total_emissions_by_scope(self, session_id: str) -> Dict[int, Dict[str, float]]:
        """
        Calcula totais de emissões por escopo.

        Utiliza funções SQL para cálculos eficientes no banco de dados.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Totais de emissões por escopo e gás

        Raises:
            RuntimeError: Se ocorrer erro ao calcular totais
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_totals_by_scope'],
                    session_id=session_id
                )

                rows = cursor.fetchall()
                if not rows:
                    return {}

                # Converte resultado para dicionário aninhado
                column_names = [col[0].lower() for col in cursor.description]
                result = {}

                for row in rows:
                    row_dict = dict(zip(column_names, row))
                    scope = row_dict.pop('scope')
                    result[scope] = row_dict

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao calcular totais de emissões: {error_obj.message}")
            raise RuntimeError(f"Falha ao calcular totais: {error_obj.message}") from e

    @with_error_handling
    def get_total_emissions_by_category(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Calcula totais de emissões por categoria dentro de cada escopo.

        Útil para relatórios detalhados e análise de fontes significativas.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Totais de emissões por escopo e categoria

        Raises:
            RuntimeError: Se ocorrer erro ao calcular totais
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_totals_by_category'],
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
            logger.error(f"Erro ao calcular totais por categoria: {error_obj.message}")
            raise RuntimeError(f"Falha ao calcular totais: {error_obj.message}") from e

    @with_error_handling
    def get_emissions_structure(self, session_id: str) -> Dict[str, Any]:
        """
        Recupera estrutura hierárquica completa de emissões.

        Organiza dados nos formatos esperados pelo protocolo GHG.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Estrutura hierárquica de emissões

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Consulta todos os dados de emissão
                cursor.execute(
                    self._queries['get_by_session'],
                    session_id=session_id
                )

                rows = cursor.fetchall()
                if not rows:
                    return {}

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                emissions_list = [dict(zip(column_names, row)) for row in rows]

                # Constrói estrutura hierárquica
                result = {}

                for emission in emissions_list:
                    scope = emission['scope']
                    category = emission['category']
                    source = emission['source']
                    gas = emission['gas']
                    value = emission['value']

                    scope_key = f"scope{scope}"

                    # Inicializa níveis da hierarquia se necessário
                    if scope_key not in result:
                        result[scope_key] = {}

                    # Para escopo 1, usamos categorias
                    if scope == 1:
                        if category not in result[scope_key]:
                            result[scope_key][category] = {}

                        if source not in result[scope_key][category]:
                            result[scope_key][category][source] = {}

                        result[scope_key][category][source][gas] = value
                    else:
                        # Escopos 2 e 3 sem categorias
                        if source not in result[scope_key]:
                            result[scope_key][source] = {}

                        result[scope_key][source][gas] = value

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao recuperar estrutura de emissões: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_emissions_time_series(self, session_id: str, gas: str = 'CO2e',
                                interval: str = 'hour') -> Dict[str, List]:
        """
        Recupera série temporal de emissões para análise de tendências.

        Args:
            session_id: Identificador da sessão
            gas: Tipo de gás para série temporal
            interval: Intervalo de agregação ('minute', 'hour', 'day')

        Returns:
            Dict: Série temporal por escopo

        Raises:
            ValueError: Se parâmetros forem inválidos
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if gas not in self.VALID_GASES:
            raise ValueError(f"Gás inválido: {gas}. Use um dos: {self.VALID_GASES}")

        # Valida intervalo
        valid_intervals = {'minute', 'hour', 'day'}
        if interval not in valid_intervals:
            raise ValueError(f"Intervalo inválido. Use um dos: {', '.join(valid_intervals)}")

        # Define formato de truncamento baseado no intervalo
        trunc_format = {
            'minute': "TRUNC(timestamp, 'MI')",
            'hour': "TRUNC(timestamp, 'HH')",
            'day': "TRUNC(timestamp, 'DD')"
        }[interval]

        # Constrói query dinâmica para série temporal
        query = f"""
            SELECT
                {trunc_format} as time_point,
                scope,
                SUM(value) as total_value
            FROM ghg_emissions
            WHERE session_id = :session_id
              AND gas = :gas
            GROUP BY {trunc_format}, scope
            ORDER BY time_point, scope
        """

        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    query,
                    session_id=session_id,
                    gas=gas
                )

                rows = cursor.fetchall()
                if not rows:
                    return {f"scope{scope}": [] for scope in self.VALID_SCOPES}

                # Organiza resultado por escopo
                result = {f"scope{scope}": [] for scope in self.VALID_SCOPES}

                for time_point, scope, value in rows:
                    scope_key = f"scope{scope}"
                    result[scope_key].append({
                        'timestamp': time_point,
                        'value': value
                    })

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar série temporal: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e
