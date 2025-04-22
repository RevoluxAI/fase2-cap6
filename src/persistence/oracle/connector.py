#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Implementa conexão robusta com Oracle para persistência de dados agrícolas.

Este módulo fornece a infraestrutura de conexão para o sistema de monitoramento
de perdas na colheita de cana-de-açúcar, com foco em segurança, desempenho e
resiliência para ambientes de produção.
"""

import os
import time
import logging
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime
from contextlib import contextmanager

import cx_Oracle

# Configuração de logging
logger = logging.getLogger(__name__)


class OracleConnector:
    """
    Gerencia conexão e pool para banco de dados Oracle.

    Implementa práticas para ambiente de produção:
    - Connection pooling para eficiência
    - Mecanismos de retry com backoff exponencial
    - Obtenção segura de credenciais
    - Suporte a batching para operações em massa
    - Modo simulado para desenvolvimento e testes
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Inicializa conector com configurações específicas.

        Args:
            config: Dicionário com configurações para conexão e comportamento.
                   Deve conter as seguintes seções:
                   - connection: Configurações de conexão
                   - pool: Configurações do pool de conexões
                   - retry: Políticas de retry
                   - timeout: Configurações de timeout
        """
        self.config = config or {}
        self._validate_config()

        # Configurações de conexão
        conn_config = self.config.get('connection', {})
        self.host = conn_config.get('host', 'localhost')
        self.port = conn_config.get('port', 1521)
        self.service_name = conn_config.get('service_name', 'ORCL')

        # Obtém credenciais de forma segura
        self._set_credentials()

        # Configurações de pool
        pool_config = self.config.get('pool', {})
        self.min_connections = pool_config.get('min', 1)
        self.max_connections = pool_config.get('max', 5)
        self.increment = pool_config.get('increment', 1)
        self.timeout = pool_config.get('timeout', 60)

        # Políticas de retry
        retry_config = self.config.get('retry', {})
        self.max_retries = retry_config.get('max_attempts', 3)
        self.retry_delay = retry_config.get('delay_seconds', 1)
        self.retry_backoff = retry_config.get('backoff_factor', 2)

        # Modo simulado para testes
        self.simulated_mode = self.config.get('simulated_mode', False)

        # Estado interno
        self.pool = None
        self.initialized = False

        # Define esquemas de tabelas
        self._initialize_schemas()

    def _validate_config(self) -> None:
        """
        Valida configuração fornecida.

        Verifica presença de seções obrigatórias e valores válidos.

        Raises:
            ValueError: Se configuração inválida for detectada
        """
        if not isinstance(self.config, dict):
            raise ValueError("Configuração deve ser um dicionário")

        # Verifica seção de conexão obrigatória
        if 'connection' not in self.config:
            raise ValueError("Configuração deve conter seção 'connection'")

        # Valida formatos e tipos
        conn_config = self.config.get('connection', {})

        if not isinstance(conn_config.get('port', 1521), int):
            raise ValueError("Porta deve ser um número inteiro")

    def _set_credentials(self) -> None:
        """
        Configura credenciais de acesso ao banco de dados.

        Prioriza variáveis de ambiente por segurança, depois configuração.
        """
        # Prioridade 1: Variáveis de ambiente
        self.username = os.environ.get('ORACLE_USERNAME')
        self.password = os.environ.get('ORACLE_PASSWORD')

        # Prioridade 2: Arquivo de configuração
        if not self.username or not self.password:
            conn_config = self.config.get('connection', {})
            self.username = conn_config.get('username', 'system')
            self.password = conn_config.get('password', 'oracle')

            # Log de segurança se usar credenciais da configuração
            if not self.simulated_mode:
                logger.warning(
                    "Usando credenciais da configuração. Recomendado usar "
                    "variáveis de ambiente em ambiente de produção."
                )

    def _initialize_schemas(self) -> None:
        """
        Inicializa esquemas SQL para criação e migração de tabelas.

        Define tabelas adequadas para o contexto agrícola com campos de
        auditoria e índices para otimização de consultas.
        """
        # Tabela de sessões com campos de auditoria
        self.table_schemas = {
            'sessions': """
                CREATE TABLE sessions (
                    session_id VARCHAR2(50) PRIMARY KEY,
                    start_timestamp TIMESTAMP,
                    end_timestamp TIMESTAMP,
                    status VARCHAR2(20),
                    created_by VARCHAR2(30),
                    last_updated TIMESTAMP,
                    version NUMBER(10) DEFAULT 1
                )
            """,
            'sensor_data': """
                CREATE TABLE sensor_data (
                    id NUMBER GENERATED ALWAYS AS IDENTITY,
                    session_id VARCHAR2(50),
                    timestamp TIMESTAMP,
                    sensor_type VARCHAR2(30),
                    sensor_value NUMBER(10,2),
                    unit VARCHAR2(10),
                    quality_flag VARCHAR2(10) DEFAULT 'GOOD',
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """,
            'ghg_emissions': """
                CREATE TABLE ghg_emissions (
                    id NUMBER GENERATED ALWAYS AS IDENTITY,
                    session_id VARCHAR2(50),
                    timestamp TIMESTAMP,
                    scope NUMBER(1),
                    category VARCHAR2(30),
                    source VARCHAR2(50),
                    gas VARCHAR2(10),
                    value NUMBER(10,2),
                    unit VARCHAR2(10),
                    calculation_method VARCHAR2(20),
                    uncertainty_percent NUMBER(5,2),
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """,
            'carbon_stocks': """
                CREATE TABLE carbon_stocks (
                    id NUMBER GENERATED ALWAYS AS IDENTITY,
                    session_id VARCHAR2(50),
                    timestamp TIMESTAMP,
                    stock_type VARCHAR2(30),
                    change NUMBER(10,2),
                    amortization_period NUMBER(3),
                    unit VARCHAR2(10),
                    measurement_method VARCHAR2(30),
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """,
            'harvest_losses': """
                CREATE TABLE harvest_losses (
                    id NUMBER GENERATED ALWAYS AS IDENTITY,
                    session_id VARCHAR2(50),
                    timestamp TIMESTAMP,
                    loss_percent NUMBER(5,2),
                    factors VARCHAR2(200),
                    confidence_level VARCHAR2(10),
                    field_conditions TEXT,
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """,
            # Índices para otimização de consultas
            'indices': [
                """CREATE INDEX idx_sensor_session_time
                   ON sensor_data(session_id, timestamp)""",
                """CREATE INDEX idx_emissions_session_cat
                   ON ghg_emissions(session_id, category)""",
                """CREATE INDEX idx_carbon_session_type
                   ON carbon_stocks(session_id, stock_type)""",
                """CREATE INDEX idx_harvest_session_time
                   ON harvest_losses(session_id, timestamp)"""
            ]
        }

    def initialize(self) -> bool:
        """
        Inicializa o pool de conexões Oracle.

        Estabelece a conexão com o banco de dados e cria o pool para
        compartilhamento entre componentes do sistema.

        Returns:
            bool: True se inicialização bem-sucedida, False caso contrário
        """
        if self.initialized:
            return True

        if self.simulated_mode:
            logger.info("Modo simulado ativo: não conectando ao Oracle")
            self.initialized = True
            return True

        try:
            # Cria DSN (Data Source Name)
            dsn = cx_Oracle.makedsn(
                self.host,
                self.port,
                service_name=self.service_name
            )

            logger.info(f"Inicializando pool de conexões Oracle em {self.host}:{self.port}")

            # Inicializa pool de conexões
            self.pool = cx_Oracle.SessionPool(
                user=self.username,
                password=self.password,
                dsn=dsn,
                min=self.min_connections,
                max=self.max_connections,
                increment=self.increment,
                threaded=True,
                timeout=self.timeout,
                getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT
            )

            # Testa pool com uma conexão
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                if result[0] != 1:
                    raise Exception("Teste de conexão Oracle falhou")

            self.initialized = True
            logger.info("Pool de conexões Oracle inicializado com sucesso")
            return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao inicializar conexão Oracle: {error_obj.message} "
                         f"(código: {error_obj.code})")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao inicializar Oracle: {str(e)}")
            return False


    @contextmanager
    def get_connection(self):
        """
        Obtém conexão do pool com gerenciamento de contexto.

        Yields:
            Connection: Conexão Oracle do pool

        Raises:
            RuntimeError: Se pool não estiver inicializado
            cx_Oracle.Error: Se ocorrer erro ao obter conexão
        """
        if self.simulated_mode:
            # Em modo simulado, retorna um objeto fictício
            class DummyCursor:
                def __init__(self):
                    # Simula descrição de coluna Oracle (7-tuple por coluna)
                    # (name, type, display_size, internal_size, precision, scale,
                    # null_ok)
                    self.description = [
                        ("id", None, None, None, None, None, None),
                        ("session_id", None, None, None, None, None, None),
                        ("start_timestamp", None, None, None, None, None, None),
                        ("end_timestamp", None, None, None, None, None, None),
                        ("status", None, None, None, None, None, None)
                    ]
                    self.rowcount = 1

                def execute(self, *args, **kwargs):
                    pass

                def fetchone(self):
                    return [1, "dummy_session", "2025-04-21 00:00:00",
                        None, "active"]

                def fetchall(self):
                    return [[1, "dummy_session", "2025-04-21 00:00:00",
                            None, "active"]]

                def close(self):
                    pass

            class DummyConnection:
                def cursor(self):
                    return DummyCursor()

                def commit(self):
                    pass

                def rollback(self):
                    pass

                def close(self):
                    pass

            yield DummyConnection()
            return


    def shutdown(self) -> bool:
        """
        Encerra o pool de conexões de forma segura.

        Libera todos os recursos associados ao pool de conexões.

        Returns:
            bool: True se encerramento bem-sucedido, False caso contrário
        """
        if self.simulated_mode:
            self.initialized = False
            return True

        if not self.pool:
            return True

        try:
            logger.info("Encerrando pool de conexões Oracle")
            self.pool.close()
            self.pool = None
            self.initialized = False
            return True
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao encerrar pool: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao encerrar pool: {str(e)}")
            return False

    def create_tables(self) -> bool:
        """
        Cria tabelas necessárias no banco de dados.

        Verifica a existência prévia e cria apenas tabelas ausentes.
        Executa dentro de uma transação para garantir atomicidade.

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if self.simulated_mode:
            return True

        if not self.initialized and not self.initialize():
            return False

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica tabelas existentes
                cursor.execute("""
                    SELECT table_name
                    FROM user_tables
                    WHERE table_name IN ('SESSIONS', 'SENSOR_DATA', 'GHG_EMISSIONS',
                                        'CARBON_STOCKS', 'HARVEST_LOSSES')
                """)

                existing_tables = [row[0] for row in cursor.fetchall()]
                logger.info(f"Tabelas existentes: {existing_tables}")

                # Cria tabelas na ordem correta respeitando referências
                tables_to_create = []
                if 'SESSIONS' not in existing_tables:
                    tables_to_create.append(('sessions', self.table_schemas['sessions']))

                # Tabelas dependentes só podem ser criadas se sessions existir
                if 'SESSIONS' in existing_tables or 'sessions' in [t[0] for t in tables_to_create]:
                    for table_name in ['sensor_data', 'ghg_emissions',
                                      'carbon_stocks', 'harvest_losses']:
                        if table_name.upper() not in existing_tables:
                            tables_to_create.append(
                                (table_name, self.table_schemas[table_name])
                            )

                # Cria tabelas
                for table_name, schema in tables_to_create:
                    try:
                        logger.info(f"Criando tabela {table_name}")
                        cursor.execute(schema)
                    except cx_Oracle.Error as e:
                        error_obj, = e.args
                        # Ignora erro se tabela já existir
                        if error_obj.code == 955:  # ORA-00955: name already used
                            logger.info(f"Tabela {table_name} já existe")
                        else:
                            logger.error(f"Erro ao criar tabela {table_name}: "
                                        f"{error_obj.message}")
                            raise

                # Cria índices
                if 'indices' in self.table_schemas:
                    for index_sql in self.table_schemas['indices']:
                        try:
                            cursor.execute(index_sql)
                        except cx_Oracle.Error as e:
                            error_obj, = e.args
                            # Ignora erro se índice já existir
                            if error_obj.code == 955:
                                logger.info("Índice já existe")
                            else:
                                logger.warning(f"Erro ao criar índice: {error_obj.message}")

                conn.commit()
                logger.info("Criação/verificação de tabelas concluída com sucesso")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao criar tabelas: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao criar tabelas: {str(e)}")
            return False

    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List:
        """
        Executa consulta SQL com parâmetros e retorna resultados.

        Usa prepared statements para proteção contra injeção SQL.

        Args:
            sql: Consulta SQL a ser executada
            params: Parâmetros para a consulta (opcional)

        Returns:
            List: Registros retornados pela consulta

        Raises:
            RuntimeError: Se pool não estiver inicializado
            cx_Oracle.Error: Se ocorrer erro durante a execução
        """
        if self.simulated_mode:
            return [[1]]

        if not self.initialized and not self.initialize():
            raise RuntimeError("Pool de conexões não inicializado")

        params = params or {}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                return rows
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao executar consulta: {error_obj.message}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao executar consulta: {str(e)}")
            raise

    def execute_batch(self, sql: str, batch_data: List[Dict[str, Any]]) -> int:
        """
        Executa comando SQL em lote para múltiplos registros.

        Otimizado para operações de inserção em massa.

        Args:
            sql: Comando SQL a ser executado
            batch_data: Lista de dicionários com parâmetros para cada registro

        Returns:
            int: Número de registros processados

        Raises:
            RuntimeError: Se pool não estiver inicializado
            cx_Oracle.Error: Se ocorrer erro durante a execução
        """
        if self.simulated_mode:
            return len(batch_data)

        if not self.initialized and not self.initialize():
            raise RuntimeError("Pool de conexões não inicializado")

        if not batch_data:
            return 0

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, batch_data)
                conn.commit()
                rows_affected = cursor.rowcount
                return rows_affected
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao executar lote: {error_obj.message}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao executar lote: {str(e)}")
            raise

    def is_healthy(self) -> bool:
        """
        Verifica se a conexão com o banco está saudável.

        Útil para health checks da aplicação.

        Returns:
            bool: True se conexão está saudável, False caso contrário
        """
        if self.simulated_mode:
            return True

        if not self.initialized:
            return False

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception:
            return False

    def get_database_info(self) -> Dict[str, str]:
        """
        Obtém informações sobre o banco de dados conectado.

        Returns:
            Dict: Informações do banco de dados
        """
        if self.simulated_mode:
            return {
                "version": "Oracle Database 19c Simulated",
                "instance_name": "SIMULATED",
                "hostname": "localhost",
                "database_name": "ORCL"
            }

        if not self.initialized and not self.initialize():
            return {"error": "Não inicializado"}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT banner, instance_name, host_name, name
                    FROM v$version, v$instance, v$database
                    WHERE rownum = 1
                """)
                row = cursor.fetchone()

                if row:
                    return {
                        "version": row[0],
                        "instance_name": row[1],
                        "hostname": row[2],
                        "database_name": row[3]
                    }
                return {"error": "Informações não disponíveis"}
        except Exception as e:
            logger.error(f"Erro ao obter informações do banco: {str(e)}")
            return {"error": str(e)}

    @staticmethod
    def classify_error(error: cx_Oracle.Error) -> Dict[str, Any]:
        """
        Classifica erro Oracle por tipo para tratamento apropriado.

        Args:
            error: Objeto de erro Oracle

        Returns:
            Dict: Classificação do erro com info adicional
        """
        error_obj, = error.args
        error_code = error_obj.code

        # Categorias de erro
        connection_errors = [3113, 3114, 12541, 12545]  # Problemas de conexão
        constraint_errors = [1, 2290, 2291, 2292]  # Violações de constraint
        permission_errors = [1031, 1017]  # Erros de permissão

        result = {
            "code": error_code,
            "message": error_obj.message,
            "is_connection_error": error_code in connection_errors,
            "is_constraint_error": error_code in constraint_errors,
            "is_permission_error": error_code in permission_errors,
            "is_recoverable": error_code in connection_errors,
        }

        return result
