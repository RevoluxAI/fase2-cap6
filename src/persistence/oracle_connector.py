#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 3. Sistema de Persistência
# src/persistence/oracle_connector.py
import os
import cx_Oracle
from datetime import datetime

class OracleConnector:
    """
    Gerencia conexão e persistência com banco de dados Oracle.
    """

    def __init__(self, config=None):
        """
        Inicializa o conector Oracle.

        Args:
            config (dict): Configurações de conexão com Oracle
        """
        self.config = config or {}
        self.connection = None
        self.cursor = None

        # Configurações de conexão
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 1521)
        self.service_name = self.config.get('service_name', 'ORCL')
        self.username = self.config.get('username', 'system')
        self.password = self.config.get('password', 'oracle')

        # Flag para modo simulado (sem conexão real)
        self.simulated_mode = self.config.get('simulated_mode', False)

        # Esquemas SQL para criação de tabelas
        self.table_schemas = {
            'sessions': """
                CREATE TABLE sessions (
                    session_id VARCHAR2(50) PRIMARY KEY,
                    start_timestamp TIMESTAMP,
                    end_timestamp TIMESTAMP,
                    status VARCHAR2(20)
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
                    factors VARCHAR2(100),
                    PRIMARY KEY (id),
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """
        }

    def connect(self):
        """
        Estabelece conexão com o banco Oracle.

        Returns:
            bool: Sucesso da conexão
        """
        if self.simulated_mode:
            print("Modo simulado: não conectando realmente ao Oracle")
            return True

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

            # Cria cursor
            self.cursor = self.connection.cursor()

            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao conectar ao Oracle: {error.message}")
            return False

    def disconnect(self):
        """
        Encerra conexão com o banco Oracle.

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        try:
            if self.cursor:
                self.cursor.close()

            if self.connection:
                self.connection.close()

            self.cursor = None
            self.connection = None

            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao desconectar do Oracle: {error.message}")
            return False

    def create_tables(self):
        """
        Cria tabelas necessárias no banco de dados.

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Cria tabelas na ordem correta respeitando referências
            for table_name in ['sessions', 'sensor_data', 'ghg_emissions',
                              'carbon_stocks', 'harvest_losses']:
                schema = self.table_schemas[table_name]

                try:
                    self.cursor.execute(schema)
                    self.connection.commit()
                except cx_Oracle.Error as e:
                    # Ignora erro se tabela já existir
                    error, = e.args
                    if 'ORA-00955' in error.message:  # Tabela já existe
                        pass
                    else:
                        raise

            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao criar tabelas: {error.message}")
            self.connection.rollback()
            return False

    def start_session(self, session_id):
        """
        Inicia uma sessão no banco de dados.

        Args:
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Insere nova sessão
            sql = """
                INSERT INTO sessions (session_id, start_timestamp, status)
                VALUES (:session_id, :start_timestamp, :status)
            """

            self.cursor.execute(
                sql,
                session_id=session_id,
                start_timestamp=datetime.now(),
                status='active'
            )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao iniciar sessão: {error.message}")
            self.connection.rollback()
            return False

    def end_session(self, session_id):
        """
        Finaliza uma sessão no banco de dados.

        Args:
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Atualiza sessão existente
            sql = """
                UPDATE sessions
                SET end_timestamp = :end_timestamp, status = :status
                WHERE session_id = :session_id
            """

            self.cursor.execute(
                sql,
                end_timestamp=datetime.now(),
                status='completed',
                session_id=session_id
            )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao finalizar sessão: {error.message}")
            self.connection.rollback()
            return False

    def save_sensor_data(self, session_id, sensor_data):
        """
        Salva dados de sensores no banco.

        Args:
            session_id (str): Identificador da sessão
            sensor_data (dict): Dados dos sensores

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Prepara SQL para inserção
            sql = """
                INSERT INTO sensor_data
                (session_id, timestamp, sensor_type, sensor_value, unit)
                VALUES (:session_id, :timestamp, :sensor_type, :sensor_value, :unit)
            """

            # Processa cada leitura de sensor
            for sensor_name, reading in sensor_data.items():
                if isinstance(reading, dict) and 'value' in reading:
                    # Formato completo com timestamp e unidade
                    self.cursor.execute(
                        sql,
                        session_id=session_id,
                        timestamp=datetime.fromisoformat(
                            reading.get('timestamp', datetime.now().isoformat())
                        ),
                        sensor_type=sensor_name,
                        sensor_value=reading['value'],
                        unit=reading.get('unit', '')
                    )
                else:
                    # Formato simplificado (apenas valor)
                    self.cursor.execute(
                        sql,
                        session_id=session_id,
                        timestamp=datetime.now(),
                        sensor_type=sensor_name,
                        sensor_value=reading,
                        unit=''
                    )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao salvar dados de sensores: {error.message}")
            self.connection.rollback()
            return False

    def save_ghg_emissions(self, session_id, emissions_data):
        """
        Salva dados de emissões GHG no banco.

        Args:
            session_id (str): Identificador da sessão
            emissions_data (dict): Dados de emissões

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Prepara SQL para inserção
            sql = """
                INSERT INTO ghg_emissions
                (session_id, timestamp, scope, category, source,
                gas, value, unit, calculation_method)
                VALUES
                (:session_id, :timestamp, :scope, :category, :source,
                :gas, :value, :unit, :calculation_method)
            """

            # Processa cada escopo de emissões
            for scope_name, scope_data in emissions_data.items():
                scope_num = int(scope_name.replace('scope', ''))

                if isinstance(scope_data, dict):
                    # Escopo 1 tem categorias
                    if scope_num == 1:
                        for category, category_data in scope_data.items():
                            for source, source_data in category_data.items():
                                for gas, value in source_data.items():
                                    # Pula entradas que não são gases
                                    if gas not in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                        continue

                                    self.cursor.execute(
                                        sql,
                                        session_id=session_id,
                                        timestamp=datetime.now(),
                                        scope=scope_num,
                                        category=category,
                                        source=source,
                                        gas=gas,
                                        value=value,
                                        unit='kg',
                                        calculation_method='tier1'
                                    )
                    else:
                        # Escopos 2 e 3 são mais simples
                        for source, source_data in scope_data.items():
                            for gas, value in source_data.items():
                                # Pula entradas que não são gases
                                if gas not in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                    continue

                                self.cursor.execute(
                                    sql,
                                    session_id=session_id,
                                    timestamp=datetime.now(),
                                    scope=scope_num,
                                    category='',
                                    source=source,
                                    gas=gas,
                                    value=value,
                                    unit='kg',
                                    calculation_method='tier1'
                                )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao salvar emissões GHG: {error.message}")
            self.connection.rollback()
            return False

    def save_carbon_stocks(self, session_id, carbon_data):
        """
        Salva dados de estoques de carbono no banco.

        Args:
            session_id (str): Identificador da sessão
            carbon_data (dict): Dados de estoques de carbono

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Prepara SQL para inserção
            sql = """
                INSERT INTO carbon_stocks
                (session_id, timestamp, stock_type, change, amortization_period, unit)
                VALUES
                (:session_id, :timestamp, :stock_type, :change,
                :amortization_period, :unit)
            """

            # Processa cada tipo de estoque
            for stock_type, stock_data in carbon_data.items():
                self.cursor.execute(
                    sql,
                    session_id=session_id,
                    timestamp=datetime.now(),
                    stock_type=stock_type,
                    change=stock_data.get('change_co2', 0),
                    amortization_period=stock_data.get('amortization_period', 20),
                    unit='kg CO2e'
                )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao salvar estoques de carbono: {error.message}")
            self.connection.rollback()
            return False

    def save_harvest_losses(self, session_id, loss_data):
        """
        Salva dados de perdas na colheita no banco.

        Args:
            session_id (str): Identificador da sessão
            loss_data (dict): Dados de perdas na colheita

        Returns:
            bool: Sucesso da operação
        """
        if self.simulated_mode:
            return True

        if not self.connection or not self.cursor:
            if not self.connect():
                return False

        try:
            # Prepara SQL para inserção
            sql = """
                INSERT INTO harvest_losses
                (session_id, timestamp, loss_percent, factors)
                VALUES
                (:session_id, :timestamp, :loss_percent, :factors)
            """

            # Extrai fatores problemáticos
            problematic_factors = loss_data.get('problematic_factors', [])
            factors_str = ','.join([f.get('factor', '') for f in problematic_factors])

            self.cursor.execute(
                sql,
                session_id=session_id,
                timestamp=datetime.now(),
                loss_percent=loss_data.get('loss_estimate', 0),
                factors=factors_str[:100]  # Limita a 100 caracteres
            )

            self.connection.commit()
            return True
        except cx_Oracle.Error as e:
            error, = e.args
            print(f"Erro ao salvar perdas na colheita: {error.message}")
            self.connection.rollback()
            return False
