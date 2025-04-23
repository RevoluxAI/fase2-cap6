# src/persistence/oracle/schema_initializer.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Inicializa o esquema do banco de dados Oracle para o sistema de monitoramento
de perdas na colheita de cana-de-açúcar.

Este módulo cria todas as tabelas necessárias com a estrutura correta,
garantindo a consistência entre o modelo de dados e o banco Oracle.
"""

import os
import sys
import logging
import argparse
import cx_Oracle

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaInitializer:
    """
    Inicializa o esquema completo do banco de dados Oracle.

    Cria todas as tabelas necessárias para persistência dos dados de
    sensores, análises, emissões e inventários relacionados à colheita
    de cana-de-açúcar.
    """

    def __init__(self, host, port, service_name, username, password):
        """
        Inicializa o inicializador de esquema com os parâmetros de conexão.

        Args:
            host: Host do servidor Oracle
            port: Porta do servidor
            service_name: Nome do serviço Oracle
            username: Nome de usuário
            password: Senha
        """
        self.host = host
        self.port = port
        self.service_name = service_name
        self.username = username
        self.password = password
        self.connection = None

        # Define esquemas SQL para criação de tabelas
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
                    quality_flag VARCHAR2(10),
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
                    factors VARCHAR2(4000),
                    confidence_level VARCHAR2(10),
                    field_conditions VARCHAR2(4000),
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """
        }

        # Define índices para otimização
        self.indices = [
            """CREATE INDEX idx_sensor_session_time
               ON sensor_data(session_id, timestamp)""",
            """CREATE INDEX idx_emissions_session
               ON ghg_emissions(session_id)""",
            """CREATE INDEX idx_carbon_session
               ON carbon_stocks(session_id)""",
            """CREATE INDEX idx_losses_session
               ON harvest_losses(session_id)"""
        ]

    def connect(self):
        """
        Estabelece conexão com o banco Oracle.

        Returns:
            bool: Sucesso da conexão
        """
        try:
            # Constrói string de conexão
            dsn = cx_Oracle.makedsn(
                self.host,
                self.port,
                service_name=self.service_name
            )

            # Estabelece conexão
            self.connection = cx_Oracle.connect(
                user=self.username,
                password=self.password,
                dsn=dsn
            )

            logger.info(f"Conectado ao Oracle em {self.host}:{self.port}")
            return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao conectar ao Oracle: {error_obj.message}")
            return False

    def create_schema(self):
        """
        Cria o esquema completo do banco de dados.

        Returns:
            bool: Sucesso da operação
        """
        if not self.connection:
            logger.error("Sem conexão com o banco de dados")
            return False

        try:
            cursor = self.connection.cursor()

            # Verifica tabelas existentes
            cursor.execute("""
                SELECT table_name
                FROM user_tables
                WHERE table_name IN ('SESSIONS', 'SENSOR_DATA', 'GHG_EMISSIONS',
                                    'CARBON_STOCKS', 'HARVEST_LOSSES')
            """)

            existing_tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Tabelas existentes: {existing_tables}")

            # Remove tabelas existentes (se necessário)
            if existing_tables:
                logger.info("Removendo tabelas existentes...")
                self._drop_existing_tables(cursor, existing_tables)

            # Cria tabelas na ordem correta respeitando referências
            logger.info("Criando tabelas...")
            for table_name in ['sessions', 'sensor_data', 'ghg_emissions',
                              'carbon_stocks', 'harvest_losses']:

                schema = self.table_schemas[table_name]
                logger.info(f"Criando tabela {table_name}...")

                try:
                    cursor.execute(schema)
                    logger.info(f"Tabela {table_name} criada com sucesso")
                except cx_Oracle.Error as e:
                    error_obj, = e.args
                    if error_obj.code == 955:  # ORA-00955: name already used
                        logger.info(f"Tabela {table_name} já existe")
                    else:
                        logger.error(f"Erro ao criar tabela {table_name}: "
                                    f"{error_obj.message}")
                        raise

            # Cria índices para otimização
            logger.info("Criando índices...")
            for index_sql in self.indices:
                try:
                    cursor.execute(index_sql)
                    logger.info("Índice criado com sucesso")
                except cx_Oracle.Error as e:
                    error_obj, = e.args
                    if error_obj.code == 955:  # Índice já existe
                        logger.info("Índice já existe")
                    else:
                        logger.warning(f"Erro ao criar índice: {error_obj.message}")

            self.connection.commit()
            logger.info("Esquema criado com sucesso")
            return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao criar esquema: {error_obj.message}")
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def _drop_existing_tables(self, cursor, existing_tables):
        """
        Remove tabelas existentes para recriação.

        Args:
            cursor: Cursor Oracle
            existing_tables: Lista de tabelas existentes
        """
        # Primeiro desabilita restrições de chaves estrangeiras
        for table in existing_tables:
            try:
                cursor.execute(f"""
                    BEGIN
                        FOR c IN (SELECT constraint_name, table_name
                                 FROM user_constraints
                                 WHERE table_name = '{table}'
                                 AND constraint_type = 'R') LOOP
                            EXECUTE IMMEDIATE 'ALTER TABLE ' || c.table_name ||
                                           ' DROP CONSTRAINT ' || c.constraint_name;
                        END LOOP;
                    END;
                """)
            except cx_Oracle.Error as e:
                error_obj, = e.args
                logger.warning(f"Erro ao desabilitar restrições: {error_obj.message}")

        # Depois remove as tabelas em ordem inversa (para evitar problemas com FK)
        for table in reversed(existing_tables):
            try:
                cursor.execute(f"DROP TABLE {table}")
                logger.info(f"Tabela {table} removida")
            except cx_Oracle.Error as e:
                error_obj, = e.args
                logger.warning(f"Erro ao remover tabela {table}: {error_obj.message}")

    def disconnect(self):
        """
        Encerra conexão com o banco Oracle.

        Returns:
            bool: Sucesso da operação
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("Conexão encerrada")
                return True
            except cx_Oracle.Error as e:
                error_obj, = e.args
                logger.error(f"Erro ao desconectar: {error_obj.message}")
                return False
        return True


def main():
    """
    Função principal para execução via linha de comando.
    """
    parser = argparse.ArgumentParser(
        description="Inicializador de Esquema Oracle para Sistema de Colheita"
    )

    parser.add_argument("--host", default="localhost",
                      help="Host do servidor Oracle")
    parser.add_argument("--port", type=int, default=1521,
                      help="Porta do servidor Oracle")
    parser.add_argument("--service", default="ORCL",
                      help="Nome do serviço Oracle")
    parser.add_argument("--user", default="system",
                      help="Usuário Oracle")
    parser.add_argument("--password",
                      help="Senha Oracle (se omitida, será solicitada)")
    parser.add_argument("--force", action="store_true",
                      help="Forçar recriação das tabelas")

    args = parser.parse_args()

    # Solicita senha se não fornecida
    password = args.password
    if not password:
        import getpass
        password = getpass.getpass("Senha Oracle: ")

    print("\nInicializando esquema do banco Oracle...")

    # Cria e executa inicializador
    initializer = SchemaInitializer(
        args.host,
        args.port,
        args.service,
        args.user,
        password
    )

    if initializer.connect():
        print("Conexão estabelecida com sucesso!")

        if initializer.create_schema():
            print("\nEsquema criado com sucesso!")
            print("As tabelas agora estão prontas para receber dados.")
        else:
            print("\nFalha ao criar esquema. Verifique os logs para detalhes.")

        initializer.disconnect()
    else:
        print("Falha ao conectar ao Oracle. Verifique os parâmetros de conexão.")


if __name__ == "__main__":
    main()
