#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de conexão com banco de dados Oracle para persistência de dados de
monitoramento de colheita de cana-de-açúcar e emissões de GHG.

Este módulo implementa um conector robusto para ambiente de produção,
com suporte a connection pooling, tratamento de erros e segurança.
"""

import os
import time
import logging
import cx_Oracle
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Union, Tuple, ContextManager
from datetime import datetime

# Configuração de logging
logger = logging.getLogger(__name__)

class OracleConnector:
    """
    Gerencia conexão e persistência com banco de dados Oracle.

    Esta classe utiliza connection pooling para eficiência e implementa
    mecanismos de retry, timeout e tratamento de erros para ambiente
    de produção.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Inicializa o conector Oracle com configurações específicas.

        Args:
            config: Dicionário com configurações de conexão e comportamento.
                   Deve conter as chaves:
                   - connection: Configuração da conexão (host, port, etc.)
                   - pool: Configuração do pool (min, max, increment)
                   - security: Configurações de segurança
                   - retry: Política de retentativas
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

        # Políticas de retry e timeout
        retry_config = self.config.get('retry', {})
        self.max_retries = retry_config.get('max_attempts', 3)
        self.retry_delay = retry_config.get('delay_seconds', 1)
        self.retry_backoff = retry_config.get('backoff_factor', 2)

        # Modo simulado para testes
        self.simulated_mode = self.config.get('simulated_mode', False)

        # Estado interno
        self.pool = None
        self.initialized = False

        # Esquemas de tabelas
        self._initialize_schemas()

    def _validate_config(self) -> None:
        """
        Valida configuração fornecida.

        Garante que as configurações necessárias estejam presentes e
        com valores válidos.

        Raises:
            ValueError: Se configuração inválida for detectada
        """
        required_sections = ['connection']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Configuração não contém seção '{section}'")

    def _set_credentials(self) -> None:
        """
        Configura credenciais de acesso ao banco de dados.

        Prioriza variáveis de ambiente, depois arquivos de configuração
        e por último valores no dicionário de configuração.
        """
        # Prioridade 1: Variáveis de ambiente
        self.username = os.environ.get('ORACLE_USERNAME')
        self.password = os.environ.get('ORACLE_PASSWORD')

        # Prioridade 2: Configuração
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

        Define esquemas de acordo com as necessidades do sistema.
        """
        # Tabela de sessões
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
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """,
            # Índices para otimização de consultas
            'indices': [
                "CREATE INDEX idx_sensor_session_time ON sensor_data(session_id, timestamp)",
                "CREATE INDEX idx_emissions_session_cat ON ghg_emissions(session_id, category)",
                "CREATE INDEX idx_carbon_session_type ON carbon_stocks(session_id, stock_type)",
                "CREATE INDEX idx_harvest_session_time ON harvest_losses(session_id, timestamp)"
            ]
        }

    def initialize(self) -> bool:
        """
        Inicializa o pool de conexões Oracle e verifica estrutura do banco.

        Returns:
            bool: Verdadeiro se inicialização foi bem-sucedida
        """
        if self.initialized:
            return True

        if self.simulated_mode:
            logger.info("Modo simulado: não conectando realmente ao Oracle")
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
    def get_connection(self) -> ContextManager[cx_Oracle.Connection]:
        """
        Obtém conexão do pool com gerenciamento de contexto.

        Implementa padrão context manager para garantir liberação da conexão.

        Returns:
            ContextManager: Gerenciador de contexto com conexão Oracle

        Raises:
            RuntimeError: Se pool não estiver inicializado
            cx_Oracle.Error: Se ocorrer erro ao obter conexão
        """
        if self.simulated_mode:
            # Em modo simulado, retorna um objeto fictício
            class DummyConnection:
                def cursor(self):
                    class DummyCursor:
                        def execute(self, *args, **kwargs):
                            pass
                        def fetchone(self):
                            return [1]
                        def fetchall(self):
                            return [[1]]
                        def close(self):
                            pass
                    return DummyCursor()
                def commit(self):
                    pass
                def rollback(self):
                    pass
                def close(self):
                    pass

            yield DummyConnection()
            return

        if not self.pool:
            raise RuntimeError("Pool de conexões não inicializado")

        connection = None
        try:
            # Aplica política de retry
            retries = 0
            delay = self.retry_delay

            while retries <= self.max_retries:
                try:
                    connection = self.pool.acquire()
                    break
                except cx_Oracle.Error as e:
                    retries += 1
                    if retries > self.max_retries:
                        raise

                    # Aplica backoff exponencial
                    logger.warning(f"Falha ao obter conexão, tentativa {retries}. "
                                  f"Aguardando {delay}s.")
                    time.sleep(delay)
                    delay *= self.retry_backoff

            # Configura ambiente da sessão se necessário
            # connection.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'")

            yield connection

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro na conexão Oracle: {error_obj.message} "
                        f"(código: {error_obj.code})")
            raise
        finally:
            if connection:
                try:
                    self.pool.release(connection)
                except Exception as e:
                    logger.error(f"Erro ao liberar conexão: {str(e)}")

    def shutdown(self) -> bool:
        """
        Encerra pool de conexões de forma segura.

        Returns:
            bool: Verdadeiro se encerramento foi bem-sucedido
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

        Verifica existência prévia e cria apenas tabelas ausentes.

        Returns:
            bool: Verdadeiro se operação foi bem-sucedida
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
                logger.info("Criação/verificação de tabelas concluída")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao criar tabelas: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao criar tabelas: {str(e)}")
            return False

    def start_session(self, session_id: str, metadata: Optional[Dict] = None) -> bool:
        """
        Inicia uma sessão no banco de dados.

        Args:
            session_id: Identificador único da sessão
            metadata: Metadados opcionais da sessão

        Returns:
            bool: Verdadeiro se operação foi bem-sucedida
        """
        if self.simulated_mode:
            return True

        if not self.initialized and not self.initialize():
            return False

        metadata = metadata or {}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica se sessão já existe
                cursor.execute(
                    "SELECT COUNT(*) FROM sessions WHERE session_id = :session_id",
                    session_id=session_id
                )

                if cursor.fetchone()[0] > 0:
                    logger.warning(f"Sessão {session_id} já existe")
                    return False

                # Insere nova sessão
                now = datetime.now()
                created_by = metadata.get('created_by', 'system')

                sql = """
                    INSERT INTO sessions (
                        session_id, start_timestamp, status,
                        created_by, last_updated, version
                    ) VALUES (
                        :session_id, :start_timestamp, :status,
                        :created_by, :last_updated, 1
                    )
                """

                cursor.execute(
                    sql,
                    session_id=session_id,
                    start_timestamp=now,
                    status='active',
                    created_by=created_by,
                    last_updated=now
                )

                conn.commit()
                logger.info(f"Sessão {session_id} iniciada com sucesso")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao iniciar sessão: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao iniciar sessão: {str(e)}")
            return False

    def end_session(self, session_id: str, metadata: Optional[Dict] = None) -> bool:
        """
        Finaliza uma sessão no banco de dados.

        Args:
            session_id: Identificador da sessão
            metadata: Metadados opcionais de encerramento

        Returns:
            bool: Verdadeiro se operação foi bem-sucedida
        """
        if self.simulated_mode:
            return True

        if not self.initialized and not self.initialize():
            return False

        metadata = metadata or {}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica se sessão existe e está ativa
                cursor.execute("""
                    SELECT status, version FROM sessions
                    WHERE session_id = :session_id
                """, session_id=session_id)

                result = cursor.fetchone()
                if not result:
                    logger.warning(f"Sessão {session_id} não encontrada")
                    return False

                status, version = result
                if status != 'active':
                    logger.warning(f"Sessão {session_id} não está ativa (status={status})")
                    return False

                # Atualiza sessão existente com otimistic locking
                now = datetime.now()

                sql = """
                    UPDATE sessions
                    SET end_timestamp = :end_timestamp,
                        status = :status,
                        last_updated = :last_updated,
                        version = :new_version
                    WHERE session_id = :session_id
                    AND version = :old_version
                """

                cursor.execute(
                    sql,
                    end_timestamp=now,
                    status='completed',
                    last_updated=now,
                    new_version=version + 1,
                    session_id=session_id,
                    old_version=version
                )

                if cursor.rowcount == 0:
                    logger.warning(f"Conflito de versão ao finalizar sessão {session_id}")
                    conn.rollback()
                    return False

                conn.commit()
                logger.info(f"Sessão {session_id} finalizada com sucesso")
                return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao finalizar sessão: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao finalizar sessão: {str(e)}")
            return False

    def save_sensor_data(self, session_id: str,
                       sensor_data: Dict[str, Any]) -> bool:
        """
        Salva dados de sensores no banco.

        Utiliza inserção em lote para maior eficiência.

        Args:
            session_id: Identificador da sessão
            sensor_data: Dados dos sensores

        Returns:
            bool: Verdadeiro se operação foi bem-sucedida
        """
        if self.simulated_mode:
            return True

        if not self.initialized and not self.initialize():
            return False

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica se sessão existe e está ativa
                cursor.execute("""
                    SELECT status FROM sessions
                    WHERE session_id = :session_id
                """, session_id=session_id)

                result = cursor.fetchone()
                if not result or result[0] != 'active':
                    logger.warning(f"Sessão {session_id} não encontrada ou não ativa")
                    return False

                # Prepara inserção em lote
                sql = """
                    INSERT INTO sensor_data (
                        session_id, timestamp, sensor_type,
                        sensor_value, unit, quality_flag
                    ) VALUES (
                        :session_id, :timestamp, :sensor_type,
                        :sensor_value, :unit, :quality_flag
                    )
                """

                # Prepara dados para inserção em lote
                batch_data = []
                now = datetime.now()

                for sensor_name, reading in sensor_data.items():
                    # Processa cada leitura de sensor
                    if isinstance(reading, dict) and 'value' in reading:
                        # Formato completo com timestamp e unidade
                        timestamp = datetime.fromisoformat(
                            reading.get('timestamp', now.isoformat())
                        )
                        sensor_value = reading['value']
                        unit = reading.get('unit', '')
                    else:
                        # Formato simplificado (apenas valor)
                        timestamp = now
                        sensor_value = reading
                        unit = ''

                    # Validação básica dos dados
                    if not isinstance(sensor_value, (int, float)):
                        continue

                    batch_data.append({
                        'session_id': session_id,
                        'timestamp': timestamp,
                        'sensor_type': sensor_name,
                        'sensor_value': sensor_value,
                        'unit': unit,
                        'quality_flag': 'GOOD'
                    })

                # Executa inserção em lote se houver dados
                if batch_data:
                    cursor.executemany(sql, batch_data)
                    conn.commit()
                    logger.info(f"Salvos {len(batch_data)} registros de sensores "
                               f"para sessão {session_id}")
                    return True
                else:
                    logger.warning("Nenhum dado válido para salvar")
                    return False

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de sensores: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar dados de sensores: {str(e)}")
            return False

    def save_ghg_emissions(self, session_id: str,
                          emissions_data: Dict[str, Any]) -> bool:
        """
        Salva dados de emissões GHG no banco.

        Args:
            session_id: Identificador da sessão
            emissions_data: Dados de emissões

        Returns:
            bool: Verdadeiro se operação foi bem-sucedida
        """
        if self.simulated_mode:
            return True

        if not self.initialized and not self.initialize():
            return False

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Verifica se sessão existe
                cursor.execute("""
                    SELECT status FROM sessions
                    WHERE session_id = :session_id
                """, session_id=session_id)

                if not cursor.fetchone():
                    logger.warning(f"Sessão {session_id} não encontrada")
                    return False

                # Prepara SQL para inserção
                sql = """
                    INSERT INTO ghg_emissions (
                        session_id, timestamp, scope, category, source,
                        gas, value, unit, calculation_method, uncertainty_percent
                    ) VALUES (
                        :session_id, :timestamp, :scope, :category, :source,
                        :gas, :value, :unit, :calculation_method, :uncertainty_percent
                    )
                """

                # Prepara dados para inserção em lote
                batch_data = []
                now = datetime.now()

                # Processa cada escopo de emissões
                for scope_name, scope_data in emissions_data.items():
                    if not isinstance(scope_data, dict):
                        continue

                    scope_num = int(scope_name.replace('scope', ''))

                    # Escopo 1 tem categorias
                    if scope_num == 1:
                        for category, category_data in scope_data.items():
                            if not isinstance(category_data, dict):
                                continue

                            for source, source_data in category_data.items():
                                if not isinstance(source_data, dict):
                                    continue

                                for gas, value in source_data.items():
                                    # Pula entradas que não são gases
                                    if gas not in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                        continue

                                    batch_data.append({
                                        'session_id': session_id,
                                        'timestamp': now,
                                        'scope': scope_num,
                                        'category': category,
                                        'source': source,
                                        'gas': gas,
                                        'value': value,
                                        'unit': 'kg',
                                        'calculation_method': 'tier1',
                                        'uncertainty_percent': 10.0  # Valor padrão
                                    })
                    else:
                        # Escopos 2 e 3 são mais simples
                        for source, source_data in scope_data.items():
                            if not isinstance(source_data, dict):
                                continue

                            for gas, value in source_data.items():
                                # Pula entradas que não são gases
                                if gas not in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                    continue

                                batch_data.append({
                                    'session_id': session_id,
                                    'timestamp': now,
                                    'scope': scope_num,
                                    'category': '',
                                    'source': source,
                                    'gas': gas,
                                    'value': value,
                                    'unit': 'kg',
                                    'calculation_method': 'tier1',
                                    'uncertainty_percent': 10.0  # Valor padrão
                                })

                # Executa inserção em lote se houver dados
                if batch_data:
                    cursor.executemany(sql, batch_data)
                    conn.commit()
                    logger.info(f"Salvos {len(batch_data)} registros de emissões GHG "
                               f"para sessão {session_id}")
                    return True
                else:
                    logger.warning("Nenhum dado válido de emissão para salvar")
                    return False

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar emissões GHG: {error_obj.message}")
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar emissões GHG: {str(e)}")
            return False

    # Outros métodos de persistência (carbon_stocks, harvest_losses) seguem
    # o mesmo padrão dos métodos anteriores, com validação, preparação e
    # inserção em lote