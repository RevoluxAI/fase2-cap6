#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia operações de dados de perdas na colheita no banco de dados Oracle.

Este módulo implementa operações CRUD para dados de perdas na colheita
mecanizada de cana-de-açúcar, incluindo percentuais de perda, fatores
problemáticos e recomendações para mitigação.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union, Tuple

import cx_Oracle

from persistence.oracle.connector import OracleConnector
from persistence.oracle.error_handler import with_error_handling, with_retry
from persistence.oracle.session_dao import SessionDAO

# Configuração de logging
logger = logging.getLogger(__name__)


class HarvestDAO:
    """
    Implementa operações de persistência para dados de perdas na colheita.

    Fornece métodos para salvar, recuperar e analisar dados de perdas
    na colheita mecanizada de cana-de-açúcar, identificando fatores
    problemáticos e suportando recomendações para mitigação.
    """

    # Constantes para validação
    VALID_CONFIDENCE_LEVELS = ['high', 'medium', 'low']

    VALID_LOSS_CATEGORIES = ['high', 'medium', 'low', 'minimal']

    KNOWN_FACTORS = [
        'harvester_speed', 'cutting_height', 'soil_humidity',
        'temperature', 'wind_speed', 'crop_density',
        'operator_skill', 'field_topography', 'machine_maintenance'
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
                INSERT INTO harvest_losses (
                    session_id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                ) VALUES (
                    :session_id, :timestamp, :loss_percent,
                    :factors, :confidence_level, :field_conditions
                )
            """,
            'get_by_id': """
                SELECT
                    id, session_id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                FROM harvest_losses
                WHERE id = :id
            """,
            'get_by_session': """
                SELECT
                    id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                FROM harvest_losses
                WHERE session_id = :session_id
                ORDER BY timestamp
            """,
            'get_latest_by_session': """
                SELECT
                    id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                FROM harvest_losses
                WHERE session_id = :session_id
                  AND ROWNUM = 1
                ORDER BY timestamp DESC
            """,
            'get_by_loss_category': """
                SELECT
                    id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                FROM harvest_losses
                WHERE session_id = :session_id
                  AND (
                      (loss_percent >= 15 AND :category = 'high') OR
                      (loss_percent >= 10 AND loss_percent < 15 AND :category = 'medium') OR
                      (loss_percent >= 5 AND loss_percent < 10 AND :category = 'low') OR
                      (loss_percent < 5 AND :category = 'minimal')
                  )
                ORDER BY timestamp
            """,
            'get_by_time_range': """
                SELECT
                    id, timestamp, loss_percent,
                    factors, confidence_level, field_conditions
                FROM harvest_losses
                WHERE session_id = :session_id
                  AND timestamp BETWEEN :start_time AND :end_time
                ORDER BY timestamp
            """,
            'get_by_factor': """
                SELECT
                    id, timestamp, loss_percent, factors, confidence_level
                FROM harvest_losses
                WHERE session_id = :session_id
                  AND factors LIKE :factor_pattern
                ORDER BY timestamp
            """,
            'get_avg_by_period': """
                SELECT
                    TRUNC(timestamp, :trunc_format) as period,
                    AVG(loss_percent) as avg_loss,
                    MIN(loss_percent) as min_loss,
                    MAX(loss_percent) as max_loss
                FROM harvest_losses
                WHERE session_id = :session_id
                GROUP BY TRUNC(timestamp, :trunc_format)
                ORDER BY period
            """,
            'get_factor_frequency': """
                SELECT
                    factors,
                    COUNT(*) as frequency
                FROM harvest_losses
                WHERE session_id = :session_id
                GROUP BY factors
                ORDER BY frequency DESC
            """
        }

    @with_error_handling
    @with_retry()
    def save_harvest_loss_data(self, session_id: str,
                             loss_data: Dict[str, Any]) -> int:
        """
        Salva dados de perdas na colheita para uma sessão específica.

        Estrutura esperada para loss_data:
        {
            "timestamp": "2025-04-10T14:30:00",  # Opcional
            "loss_estimate": 12.5,  # Percentual de perda
            "loss_category": "medium",  # Classificação de perda
            "problematic_factors": [
                {
                    "factor": "harvester_speed",
                    "value": 7.5,
                    "optimal_range": [4.5, 6.5],
                    "severity": 0.8,
                    "direction": "above"
                }
            ],
            "confidence_level": "high",  # Opcional
            "field_conditions": {  # Opcional
                "crop_variety": "RB92579",
                "field_slope": 5.2,
                "crop_yield": 85.3
            }
        }

        Args:
            session_id: Identificador da sessão
            loss_data: Dicionário com dados de perdas na colheita

        Returns:
            int: ID do registro salvo

        Raises:
            ValueError: Se dados ou sessão forem inválidos
            RuntimeError: Se ocorrer erro ao salvar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        # Valida sessão se session_dao estiver disponível
        if self.session_dao and not self.session_dao.validate_session(session_id):
            raise ValueError(f"Sessão {session_id} inválida ou não está ativa")

        if not loss_data:
            raise ValueError("Nenhum dado de perda fornecido para salvar")

        # Extrai e valida dados
        try:
            timestamp = datetime.fromisoformat(
                loss_data.get('timestamp', datetime.now().isoformat())
            )
        except (ValueError, TypeError):
            timestamp = datetime.now()

        # Percentual de perda é obrigatório
        loss_percent = loss_data.get('loss_estimate')
        if loss_percent is None:
            raise ValueError("Percentual de perda não fornecido")

        if not isinstance(loss_percent, (int, float)):
            raise ValueError(f"Percentual de perda inválido: {loss_percent}")

        if loss_percent < 0 or loss_percent > 100:
            raise ValueError(f"Percentual de perda fora da faixa válida: {loss_percent}")

        # Processa fatores problemáticos
        problematic_factors = loss_data.get('problematic_factors', [])
        if not isinstance(problematic_factors, list):
            problematic_factors = []

        # Converte fatores para formato JSON
        factors_json = json.dumps(problematic_factors) if problematic_factors else ""

        # Valida nível de confiança
        confidence_level = loss_data.get('confidence_level', 'medium')
        if confidence_level not in self.VALID_CONFIDENCE_LEVELS:
            confidence_level = 'medium'

        # Processa condições de campo
        field_conditions = loss_data.get('field_conditions', {})
        field_conditions_json = json.dumps(field_conditions) if field_conditions else ""

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Executa inserção
                cursor.execute(
                    self._queries['insert'],
                    session_id=session_id,
                    timestamp=timestamp,
                    loss_percent=loss_percent,
                    factors=factors_json,
                    confidence_level=confidence_level,
                    field_conditions=field_conditions_json
                )

                # Obtém ID do registro inserido
                cursor.execute(
                    "SELECT MAX(id) FROM harvest_losses WHERE session_id = :session_id",
                    session_id=session_id
                )
                record_id = cursor.fetchone()[0]

                conn.commit()
                logger.info(f"Salvo registro de perda na colheita (ID: {record_id}) "
                          f"para sessão {session_id}")
                return record_id

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de perda: {error_obj.message}")
            raise RuntimeError(f"Falha ao salvar dados: {error_obj.message}") from e

    @with_error_handling
    def get_harvest_loss_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """
        Recupera registro específico de perda na colheita.

        Args:
            record_id: ID do registro

        Returns:
            Dict: Dados do registro ou None se não encontrado

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_id'],
                    id=record_id
                )

                row = cursor.fetchone()
                if not row:
                    return None

                # Converte resultado para dicionário
                column_names = [col[0].lower() for col in cursor.description]
                result = dict(zip(column_names, row))

                # Processa campos JSON
                self._process_json_fields(result)

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar registro {record_id}: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_harvest_losses_by_session(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Recupera todos os registros de perdas para uma sessão.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Lista de registros de perdas

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
                result = [dict(zip(column_names, row)) for row in rows]

                # Processa campos JSON em cada registro
                for record in result:
                    self._process_json_fields(record)

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar perdas da sessão {session_id}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_latest_harvest_loss(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera o registro mais recente de perda na colheita.

        Útil para dashboards em tempo real e alertas.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Registro mais recente ou None se não encontrado

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_latest_by_session'],
                    session_id=session_id
                )

                row = cursor.fetchone()
                if not row:
                    return None

                # Converte resultado para dicionário
                column_names = [col[0].lower() for col in cursor.description]
                result = dict(zip(column_names, row))

                # Processa campos JSON
                self._process_json_fields(result)

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar perda recente: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_losses_by_category(self, session_id: str,
                             category: str) -> List[Dict[str, Any]]:
        """
        Recupera perdas por categoria de severidade.

        Args:
            session_id: Identificador da sessão
            category: Categoria de perda (high, medium, low, minimal)

        Returns:
            List: Lista de registros na categoria

        Raises:
            ValueError: Se categoria for inválida
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if category not in self.VALID_LOSS_CATEGORIES:
            raise ValueError(f"Categoria inválida: {category}. "
                           f"Use uma das: {self.VALID_LOSS_CATEGORIES}")

        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_loss_category'],
                    session_id=session_id,
                    category=category
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                result = [dict(zip(column_names, row)) for row in rows]

                # Processa campos JSON em cada registro
                for record in result:
                    self._process_json_fields(record)

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar perdas por categoria: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_losses_by_factor(self, session_id: str,
                           factor: str) -> List[Dict[str, Any]]:
        """
        Recupera perdas que envolvem um fator específico.

        Args:
            session_id: Identificador da sessão
            factor: Fator específico (ex: 'harvester_speed')

        Returns:
            List: Lista de registros com o fator

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Usa padrão LIKE para buscar fator no JSON armazenado
                factor_pattern = f"%{factor}%"

                cursor.execute(
                    self._queries['get_by_factor'],
                    session_id=session_id,
                    factor_pattern=factor_pattern
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                result = [dict(zip(column_names, row)) for row in rows]

                # Processa campos JSON em cada registro
                for record in result:
                    self._process_json_fields(record)

                # Filtra registros que realmente contêm o fator especificado
                filtered_result = []
                for record in result:
                    if 'problematic_factors' in record:
                        for factor_data in record['problematic_factors']:
                            if factor_data.get('factor') == factor:
                                filtered_result.append(record)
                                break

                return filtered_result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar perdas por fator: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_harvest_loss_trend(self, session_id: str,
                             interval: str = 'hour') -> List[Dict[str, Any]]:
        """
        Calcula tendência de perdas ao longo do tempo.

        Args:
            session_id: Identificador da sessão
            interval: Intervalo de agrupamento ('minute', 'hour', 'day')

        Returns:
            List: Dados de tendência por intervalo

        Raises:
            ValueError: Se intervalo for inválido
            RuntimeError: Se ocorrer erro ao calcular tendência
        """
        # Valida intervalo
        valid_intervals = {'minute': 'MI', 'hour': 'HH', 'day': 'DD'}
        if interval not in valid_intervals:
            raise ValueError(f"Intervalo inválido. Use um dos: {', '.join(valid_intervals)}")

        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_avg_by_period'],
                    session_id=session_id,
                    trunc_format=valid_intervals[interval]
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao calcular tendência: {error_obj.message}")
            raise RuntimeError(f"Falha ao calcular tendência: {error_obj.message}") from e

    @with_error_handling
    def analyze_common_factors(self, session_id: str,
                              threshold: float = 0.0) -> List[Dict[str, Any]]:
        """
        Analisa fatores problemáticos mais comuns.

        Identifica os fatores que mais contribuem para perdas.

        Args:
            session_id: Identificador da sessão
            threshold: Limite percentual para incluir fator (0.0 a 1.0)

        Returns:
            List: Fatores comuns com contagem e percentual

        Raises:
            RuntimeError: Se ocorrer erro na análise
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            # Recupera todos os registros de perdas
            loss_records = self.get_harvest_losses_by_session(session_id)

            if not loss_records:
                return []

            # Contagem de fatores
            factor_counts = {}
            total_records = len(loss_records)

            # Processa cada registro
            for record in loss_records:
                if 'problematic_factors' not in record:
                    continue

                for factor_data in record['problematic_factors']:
                    factor = factor_data.get('factor')
                    if not factor:
                        continue

                    if factor not in factor_counts:
                        factor_counts[factor] = 0

                    factor_counts[factor] += 1

            # Calcula percentuais e filtra por threshold
            result = []
            for factor, count in factor_counts.items():
                percentage = count / total_records

                if percentage >= threshold:
                    result.append({
                        'factor': factor,
                        'count': count,
                        'percentage': percentage
                    })

            # Ordena por contagem (maior primeiro)
            result.sort(key=lambda x: x['count'], reverse=True)

            return result

        except Exception as e:
            logger.error(f"Erro ao analisar fatores comuns: {str(e)}")
            raise RuntimeError(f"Falha na análise: {str(e)}") from e

    @with_error_handling
    def calculate_loss_statistics(self, session_id: str) -> Dict[str, Any]:
        """
        Calcula estatísticas gerais de perdas na colheita.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Estatísticas calculadas

        Raises:
            RuntimeError: Se ocorrer erro nos cálculos
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            # Recupera todos os registros de perdas
            loss_records = self.get_harvest_losses_by_session(session_id)

            if not loss_records:
                return {
                    'count': 0,
                    'avg_loss': 0.0,
                    'min_loss': 0.0,
                    'max_loss': 0.0,
                    'trend': 'insufficient_data'
                }

            # Extrai percentuais de perda
            loss_values = [record['loss_percent'] for record in loss_records]

            # Cálculos básicos
            avg_loss = sum(loss_values) / len(loss_values)
            min_loss = min(loss_values)
            max_loss = max(loss_values)

            # Analisa tendência
            trend = 'stable'
            if len(loss_values) >= 3:
                # Cálculo de tendência simples (comparando primeira e segunda metade)
                mid_point = len(loss_values) // 2
                first_half_avg = sum(loss_values[:mid_point]) / mid_point
                second_half_avg = sum(loss_values[mid_point:]) / (len(loss_values) - mid_point)

                if second_half_avg > first_half_avg * 1.1:
                    trend = 'increasing'
                elif second_half_avg < first_half_avg * 0.9:
                    trend = 'decreasing'

            # Categorização das perdas
            categories = {
                'high': 0,
                'medium': 0,
                'low': 0,
                'minimal': 0
            }

            for loss in loss_values:
                if loss >= 15:
                    categories['high'] += 1
                elif loss >= 10:
                    categories['medium'] += 1
                elif loss >= 5:
                    categories['low'] += 1
                else:
                    categories['minimal'] += 1

            # Percentuais por categoria
            total_records = len(loss_values)
            category_percentages = {
                cat: (count / total_records) * 100
                for cat, count in categories.items()
            }

            return {
                'count': len(loss_values),
                'avg_loss': avg_loss,
                'min_loss': min_loss,
                'max_loss': max_loss,
                'trend': trend,
                'categories': categories,
                'category_percentages': category_percentages
            }

        except Exception as e:
            logger.error(f"Erro ao calcular estatísticas: {str(e)}")
            raise RuntimeError(f"Falha nos cálculos: {str(e)}") from e

    def _process_json_fields(self, record: Dict[str, Any]) -> None:
        """
        Processa campos JSON em um registro.

        Converte strings JSON em objetos Python.

        Args:
            record: Registro a ser processado
        """
        # Processa campo de fatores
        if 'factors' in record and record['factors']:
            try:
                record['problematic_factors'] = json.loads(record['factors'])
                del record['factors']
            except json.JSONDecodeError:
                record['problematic_factors'] = []
        else:
            record['problematic_factors'] = []

        # Processa campo de condições de campo
        if 'field_conditions' in record and record['field_conditions']:
            try:
                record['field_conditions'] = json.loads(record['field_conditions'])
            except json.JSONDecodeError:
                record['field_conditions'] = {}
        else:
            record['field_conditions'] = {}
