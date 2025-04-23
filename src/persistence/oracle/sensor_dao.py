#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia operações de dados de sensores no banco de dados Oracle.

Este módulo implementa operações CRUD para dados de sensores coletados
durante o monitoramento da colheita mecanizada de cana-de-açúcar, incluindo
sensores ambientais, operacionais e de emissões.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

import cx_Oracle

from persistence.oracle.connector import OracleConnector
from persistence.oracle.error_handler import with_error_handling, with_retry
from persistence.oracle.session_dao import SessionDAO

# Configuração de logging
logger = logging.getLogger(__name__)


class SensorDAO:
    """
    Implementa operações de persistência para dados de sensores.

    Fornece métodos para salvar, recuperar e analisar dados coletados
    pelos sensores durante o monitoramento da colheita.
    """

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
                INSERT INTO sensor_data (
                    session_id, timestamp, sensor_type,
                    sensor_value, unit, quality_flag
                ) VALUES (
                    :session_id, :timestamp, :sensor_type,
                    :sensor_value, :unit, :quality_flag
                )
            """,
            'get_by_id': """
                SELECT
                    id, session_id, timestamp, sensor_type,
                    sensor_value, unit, quality_flag
                FROM sensor_data
                WHERE id = :id
            """,
            'get_by_session': """
                SELECT
                    id, timestamp, sensor_type,
                    sensor_value, unit, quality_flag
                FROM sensor_data
                WHERE session_id = :session_id
                ORDER BY timestamp, sensor_type
            """,
            'get_by_session_and_type': """
                SELECT
                    id, timestamp, sensor_value, unit, quality_flag
                FROM sensor_data
                WHERE session_id = :session_id
                  AND sensor_type = :sensor_type
                ORDER BY timestamp
            """,
            'get_by_time_range': """
                SELECT
                    id, session_id, timestamp, sensor_type,
                    sensor_value, unit, quality_flag
                FROM sensor_data
                WHERE session_id = :session_id
                  AND timestamp BETWEEN :start_time AND :end_time
                ORDER BY timestamp, sensor_type
            """,
            'get_latest_by_type': """
                SELECT
                    id, timestamp, sensor_value, unit, quality_flag
                FROM sensor_data
                WHERE session_id = :session_id
                  AND sensor_type = :sensor_type
                  AND ROWNUM = 1
                ORDER BY timestamp DESC
            """,
            'get_statistics': """
                SELECT
                    sensor_type,
                    COUNT(*) as count,
                    MIN(sensor_value) as min_value,
                    MAX(sensor_value) as max_value,
                    AVG(sensor_value) as avg_value,
                    MEDIAN(sensor_value) as median_value,
                    STDDEV(sensor_value) as stddev_value
                FROM sensor_data
                WHERE session_id = :session_id
                GROUP BY sensor_type
            """
        }

    @with_error_handling
    @with_retry()
    def save_sensor_data(self, session_id: str,
                        sensor_data: Dict[str, Any]) -> int:
        """
        Salva dados de sensores para uma sessão específica.

        Suporta múltiplos formatos de entrada para flexibilidade:
        - Valor simples: {"temperatura": 25.5}
        - Objeto completo: {"temperatura": {"value": 25.5, "unit": "°C",
                            "timestamp": "2023-01-01T12:00:00"}}

        Args:
            session_id: Identificador da sessão
            sensor_data: Dicionário com dados dos sensores

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

        if not sensor_data:
            logger.warning("Nenhum dado de sensor fornecido para salvar")
            return 0

        # Prepara dados para inserção em lote
        batch_data = []
        now = datetime.now()

        for sensor_name, reading in sensor_data.items():
            # Processa cada leitura de sensor
            if isinstance(reading, dict) and 'value' in reading:
                # Formato completo com timestamp e unidade
                try:
                    timestamp = datetime.fromisoformat(
                        reading.get('timestamp', now.isoformat())
                    )
                except (ValueError, TypeError):
                    timestamp = now

                sensor_value = reading['value']
                unit = reading.get('unit', '')
                quality_flag = reading.get('quality_flag', 'GOOD')
            else:
                # Formato simplificado (apenas valor)
                timestamp = now
                sensor_value = reading
                unit = ''
                quality_flag = 'GOOD'

            # Validação básica dos dados
            if not isinstance(sensor_value, (int, float)):
                logger.warning(
                    f"Valor inválido para sensor {sensor_name}: {sensor_value}. "
                    f"Ignorando."
                )
                continue

            batch_data.append({
                'session_id': session_id,
                'timestamp': timestamp,
                'sensor_type': sensor_name,
                'sensor_value': sensor_value,
                'unit': unit,
                'quality_flag': quality_flag
            })

        # Executa inserção em lote se houver dados
        if not batch_data:
            logger.warning("Nenhum dado válido de sensor para salvar")
            return 0

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Executa inserção em lote
                cursor.executemany(self._queries['insert'], batch_data)

                conn.commit()
                records_saved = len(batch_data)
                logger.info(f"Salvos {records_saved} registros de sensores "
                          f"para sessão {session_id}")
                return records_saved

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de sensores: {error_obj.message}")
            raise RuntimeError(f"Falha ao salvar dados: {error_obj.message}") from e

    @with_error_handling
    def get_sensor_data_by_session(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Recupera todos os dados de sensores para uma sessão.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Lista de registros de sensores

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
            logger.error(f"Erro ao consultar sensores da sessão {session_id}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_sensor_data_by_type(self, session_id: str,
                               sensor_type: str) -> List[Dict[str, Any]]:
        """
        Recupera dados de um tipo específico de sensor.

        Args:
            session_id: Identificador da sessão
            sensor_type: Tipo de sensor (ex: 'temperature', 'humidity')

        Returns:
            List: Lista de leituras do sensor

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_session_and_type'],
                    session_id=session_id,
                    sensor_type=sensor_type
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar sensor {sensor_type} da sessão {session_id}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_sensor_data_by_time_range(self, session_id: str,
                                     start_time: datetime,
                                     end_time: datetime) -> List[Dict[str, Any]]:
        """
        Recupera dados de sensores em um intervalo de tempo.

        Útil para analisar eventos específicos durante a colheita.

        Args:
            session_id: Identificador da sessão
            start_time: Timestamp inicial
            end_time: Timestamp final

        Returns:
            List: Lista de leituras dos sensores no intervalo

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_time_range'],
                    session_id=session_id,
                    start_time=start_time,
                    end_time=end_time
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar sensores por intervalo de tempo: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_latest_sensor_reading(self, session_id: str,
                                 sensor_type: str) -> Optional[Dict[str, Any]]:
        """
        Recupera leitura mais recente de um sensor específico.

        Útil para dashboards em tempo real e alertas.

        Args:
            session_id: Identificador da sessão
            sensor_type: Tipo de sensor

        Returns:
            Dict: Leitura mais recente ou None se não encontrada

        Raises:
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_latest_by_type'],
                    session_id=session_id,
                    sensor_type=sensor_type
                )

                row = cursor.fetchone()
                if not row:
                    return None

                # Converte resultado para dicionário
                column_names = [col[0].lower() for col in cursor.description]
                return dict(zip(column_names, row))

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar leitura recente do sensor {sensor_type}: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    @with_error_handling
    def get_sensor_statistics(self, session_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Calcula estatísticas para todos os tipos de sensores na sessão.

        Utiliza funções estatísticas do Oracle para cálculos eficientes.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Estatísticas por tipo de sensor

        Raises:
            RuntimeError: Se ocorrer erro ao calcular estatísticas
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_statistics'],
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
                    sensor_type = row_dict.pop('sensor_type')
                    result[sensor_type] = row_dict

                return result

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao calcular estatísticas de sensores: "
                       f"{error_obj.message}")
            raise RuntimeError(f"Falha ao calcular estatísticas: {error_obj.message}") from e

    @with_error_handling
    def get_aggregated_sensor_data(self, session_id: str, sensor_type: str,
                                 interval: str = 'hour') -> List[Dict[str, Any]]:
        """
        Recupera dados agregados de sensores por intervalo de tempo.

        Útil para visualizações e análises com dados reduzidos.

        Args:
            session_id: Identificador da sessão
            sensor_type: Tipo de sensor
            interval: Intervalo de agregação ('minute', 'hour', 'day')

        Returns:
            List: Dados agregados por intervalo

        Raises:
            ValueError: Se intervalo for inválido
            RuntimeError: Se ocorrer erro ao consultar dados
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

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

        # Constrói query dinâmica para agregação
        query = f"""
            SELECT
                {trunc_format} as interval_start,
                COUNT(*) as sample_count,
                AVG(sensor_value) as avg_value,
                MIN(sensor_value) as min_value,
                MAX(sensor_value) as max_value,
                MEDIAN(sensor_value) as median_value,
                STDDEV(sensor_value) as stddev_value
            FROM sensor_data
            WHERE session_id = :session_id
              AND sensor_type = :sensor_type
            GROUP BY {trunc_format}
            ORDER BY interval_start
        """

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    query,
                    session_id=session_id,
                    sensor_type=sensor_type
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar dados agregados: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar dados: {error_obj.message}") from e

    def get_sensor_types(self, session_id: str) -> List[str]:
        """
        Recupera tipos de sensores disponíveis para uma sessão.

        Args:
            session_id: Identificador da sessão

        Returns:
            List: Lista de tipos de sensores
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT DISTINCT sensor_type
                    FROM sensor_data
                    WHERE session_id = :session_id
                    ORDER BY sensor_type
                """, session_id=session_id)

                rows = cursor.fetchall()
                return [row[0] for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar tipos de sensores: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar tipos: {error_obj.message}") from e
