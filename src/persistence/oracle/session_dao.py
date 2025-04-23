#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerencia operações de sessão no banco de dados Oracle.

Este módulo implementa operações CRUD para sessões de coleta de dados,
que são fundamentais para rastrear períodos de monitoramento no sistema
de análise de perdas na colheita de cana-de-açúcar.
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

import cx_Oracle

from persistence.oracle.connector import OracleConnector
from persistence.oracle.error_handler import with_error_handling, with_retry

# Configuração de logging
logger = logging.getLogger(__name__)


class SessionDAO:
    """
    Implementa operações de persistência para sessões de monitoramento.

    Fornece métodos para criar, recuperar, atualizar e encerrar sessões
    de coleta de dados no banco Oracle.
    """

    def __init__(self, connector: OracleConnector):
        """
        Inicializa DAO com conector Oracle.

        Args:
            connector: Conector Oracle já inicializado
        """
        self.connector = connector

        # Queries SQL para operações comuns
        self._queries = {
            'create': """
                INSERT INTO sessions (
                    session_id, start_timestamp, status,
                    created_by, last_updated, version
                ) VALUES (
                    :session_id, :start_timestamp, :status,
                    :created_by, :last_updated, 1
                )
            """,
            'get_by_id': """
                SELECT
                    session_id, start_timestamp, end_timestamp,
                    status, created_by, last_updated, version
                FROM sessions
                WHERE session_id = :session_id
            """,
            'update_status': """
                UPDATE sessions
                SET status = :status,
                    last_updated = :last_updated,
                    version = version + 1
                WHERE session_id = :session_id
                  AND version = :version
            """,
            'end_session': """
                UPDATE sessions
                SET end_timestamp = :end_timestamp,
                    status = :status,
                    last_updated = :last_updated,
                    version = version + 1
                WHERE session_id = :session_id
                  AND version = :version
            """,
            'list_active': """
                SELECT
                    session_id, start_timestamp, status,
                    created_by, last_updated
                FROM sessions
                WHERE status = 'active'
                ORDER BY start_timestamp DESC
            """,
            'list_by_date_range': """
                SELECT
                    session_id, start_timestamp, end_timestamp,
                    status, created_by, last_updated
                FROM sessions
                WHERE start_timestamp BETWEEN :start_date AND :end_date
                ORDER BY start_timestamp DESC
            """
        }

    @with_error_handling
    @with_retry()
    def create_session(self, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Cria nova sessão de coleta de dados.

        Gera identificador único para rastrear todos os dados coletados
        durante esta sessão de monitoramento.

        Args:
            metadata: Metadados opcionais da sessão (criador, descrição, etc.)

        Returns:
            str: Identificador da sessão criada

        Raises:
            RuntimeError: Se ocorrer erro ao criar sessão
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        metadata = metadata or {}
        session_id = metadata.get('session_id') or self._generate_session_id()
        created_by = metadata.get('created_by', 'system')
        timestamp = datetime.now()

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica se sessão já existe
                cursor.execute(
                    self._queries['get_by_id'],
                    session_id=session_id
                )

                if cursor.fetchone():
                    raise ValueError(f"Sessão {session_id} já existe")

                # Insere nova sessão
                cursor.execute(
                    self._queries['create'],
                    session_id=session_id,
                    start_timestamp=timestamp,
                    status='active',
                    created_by=created_by,
                    last_updated=timestamp
                )

                conn.commit()
                logger.info(f"Sessão {session_id} criada com sucesso")
                return session_id

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao criar sessão: {error_obj.message}")
            raise RuntimeError(f"Falha ao criar sessão: {error_obj.message}") from e

    @with_error_handling
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera informações de uma sessão específica.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Dados da sessão ou None se não encontrada

        Raises:
            RuntimeError: Se ocorrer erro ao consultar sessão
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['get_by_id'],
                    session_id=session_id
                )

                row = cursor.fetchone()
                if not row:
                    return None

                # Converte resultado para dicionário
                column_names = [col[0].lower() for col in cursor.description]
                return dict(zip(column_names, row))

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao consultar sessão {session_id}: {error_obj.message}")
            raise RuntimeError(f"Falha ao consultar sessão: {error_obj.message}") from e

    @with_error_handling
    @with_retry()
    def update_status(self, session_id: str, status: str) -> bool:
        """
        Atualiza status de uma sessão.

        Utiliza versioning para controle de concorrência otimista.

        Args:
            session_id: Identificador da sessão
            status: Novo status (active, paused, completed, aborted)

        Returns:
            bool: True se atualização bem-sucedida, False caso contrário

        Raises:
            RuntimeError: Se ocorrer erro ao atualizar sessão
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        # Valida status
        valid_statuses = ['active', 'paused', 'completed', 'aborted']
        if status not in valid_statuses:
            raise ValueError(f"Status inválido. Use um dos: {', '.join(valid_statuses)}")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Primeiro consulta dados atuais para obter versão
                cursor.execute(
                    self._queries['get_by_id'],
                    session_id=session_id
                )

                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Sessão {session_id} não encontrada")
                    return False

                # Extrai versão atual
                column_names = [col[0].lower() for col in cursor.description]
                session_data = dict(zip(column_names, row))
                current_version = session_data.get('version', 1)

                # Atualiza status com controle de versão
                timestamp = datetime.now()
                cursor.execute(
                    self._queries['update_status'],
                    session_id=session_id,
                    status=status,
                    last_updated=timestamp,
                    version=current_version
                )

                # Verifica se realmente atualizou
                if cursor.rowcount == 0:
                    logger.warning(
                        f"Conflito de concorrência ao atualizar sessão {session_id}. "
                        f"Versão atual diferente de {current_version}."
                    )
                    return False

                conn.commit()
                logger.info(f"Status da sessão {session_id} atualizado para {status}")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao atualizar sessão {session_id}: {error_obj.message}")
            raise RuntimeError(f"Falha ao atualizar sessão: {error_obj.message}") from e

    @with_error_handling
    @with_retry()
    def end_session(self, session_id: str, status: str = 'completed') -> bool:
        """
        Encerra uma sessão de coleta de dados.

        Define timestamp de término e status final da sessão.

        Args:
            session_id: Identificador da sessão
            status: Status final (completed ou aborted)

        Returns:
            bool: True se encerramento bem-sucedido, False caso contrário

        Raises:
            RuntimeError: Se ocorrer erro ao encerrar sessão
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        # Valida status
        valid_statuses = ['completed', 'aborted']
        if status not in valid_statuses:
            raise ValueError(f"Status inválido para encerramento. Use um dos: "
                            f"{', '.join(valid_statuses)}")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                # Primeiro consulta dados atuais para verificar se pode encerrar
                cursor.execute(
                    self._queries['get_by_id'],
                    session_id=session_id
                )

                row = cursor.fetchone()
                if not row:
                    logger.warning(f"Sessão {session_id} não encontrada")
                    return False

                # Extrai dados atuais
                column_names = [col[0].lower() for col in cursor.description]
                session_data = dict(zip(column_names, row))
                current_version = session_data.get('version', 1)
                current_status = session_data.get('status')
                end_timestamp = session_data.get('end_timestamp')

                # Verifica se já está encerrada
                if end_timestamp is not None or current_status in ['completed', 'aborted']:
                    logger.warning(f"Sessão {session_id} já está encerrada")
                    return False

                # Encerra sessão
                timestamp = datetime.now()
                cursor.execute(
                    self._queries['end_session'],
                    session_id=session_id,
                    end_timestamp=timestamp,
                    status=status,
                    last_updated=timestamp,
                    version=current_version
                )

                # Verifica se realmente atualizou
                if cursor.rowcount == 0:
                    logger.warning(
                        f"Conflito de concorrência ao encerrar sessão {session_id}. "
                        f"Versão atual diferente de {current_version}."
                    )
                    return False

                conn.commit()
                logger.info(f"Sessão {session_id} encerrada com status {status}")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao encerrar sessão {session_id}: {error_obj.message}")
            raise RuntimeError(f"Falha ao encerrar sessão: {error_obj.message}") from e

    @with_error_handling
    def list_active_sessions(self) -> List[Dict[str, Any]]:
        """
        Lista todas as sessões ativas.

        Returns:
            List: Lista de sessões ativas

        Raises:
            RuntimeError: Se ocorrer erro ao listar sessões
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(self._queries['list_active'])

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao listar sessões ativas: {error_obj.message}")
            raise RuntimeError(f"Falha ao listar sessões: {error_obj.message}") from e

    @with_error_handling
    def get_sessions_by_date_range(self, start_date: datetime,
                                 end_date: datetime) -> List[Dict[str, Any]]:
        """
        Recupera sessões dentro de um intervalo de datas.

        Útil para análises históricas e relatórios de período.

        Args:
            start_date: Data inicial
            end_date: Data final

        Returns:
            List: Lista de sessões no intervalo

        Raises:
            RuntimeError: Se ocorrer erro ao consultar sessões
        """
        if not self.connector.initialized and not self.connector.initialize():
            raise RuntimeError("Conector Oracle não está inicializado")

        try:
            with self.connector.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    self._queries['list_by_date_range'],
                    start_date=start_date,
                    end_date=end_date
                )

                rows = cursor.fetchall()
                if not rows:
                    return []

                # Converte resultado para lista de dicionários
                column_names = [col[0].lower() for col in cursor.description]
                return [dict(zip(column_names, row)) for row in rows]

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao listar sessões por data: {error_obj.message}")
            raise RuntimeError(f"Falha ao listar sessões: {error_obj.message}") from e

    def _generate_session_id(self) -> str:
        """
        Gera identificador único para sessão.

        Combina timestamp e UUID para garantir unicidade.

        Returns:
            str: Identificador único para sessão
        """
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        unique_id = str(uuid.uuid4())[:8]  # Primeiros 8 caracteres do UUID
        return f"{timestamp}-{unique_id}"

    def validate_session(self, session_id: str) -> bool:
        """
        Verifica se sessão existe e está ativa.

        Útil para validação antes de operações que dependem de sessão ativa.

        Args:
            session_id: Identificador da sessão

        Returns:
            bool: True se sessão é válida e ativa, False caso contrário
        """
        session_data = self.get_session(session_id)
        if not session_data:
            return False

        # Sessão deve estar ativa
        if session_data.get('status') != 'active':
            return False

        # Sessão não deve ter timestamp de encerramento
        if session_data.get('end_timestamp') is not None:
            return False

        return True
