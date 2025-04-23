# src/persistence/oracle/oracle_service.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementa o serviço de persistência Oracle para dados de colheita.

Este módulo fornece funcionalidades para conectar, validar e persistir
dados relacionados à colheita de cana-de-açúcar no banco Oracle,
com tratamento adequado de erros e conversão de tipos.
"""

import logging
import os
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Union

import cx_Oracle

# Configuração de logging
logger = logging.getLogger(__name__)

class OracleService:
    """
    Gerencia operações de persistência no Oracle para dados de colheita.

    Implementa métodos para salvar dados de sensores, análises e inventário GHG
    no banco Oracle com validação adequada e tratamento de erros.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Inicializa o serviço Oracle com configurações fornecidas.

        Args:
            config: Configurações de conexão Oracle e comportamento
        """
        self.config = config or {}
        self.connection = None
        self.initialized = False

        # Configurações de conexão
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 1521)
        self.service_name = self.config.get('service_name', 'ORCL')
        self.username = self.config.get('username', 'system')
        self.password = self.config.get('password', 'oracle')

        # Modo simulado para desenvolvimento/testes
        self.simulated_mode = self.config.get('simulated_mode', False)

        # Opções de validação
        self.validate_data = self.config.get('validate_data', True)
        self.max_string_length = self.config.get('max_string_length', 4000)

        # Configuração de retry para operações
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay = self.config.get('retry_delay', 1)

    def initialize(self) -> bool:
        """
        Inicializa o serviço Oracle, estabelecendo conexão e criando tabelas.

        Returns:
            bool: True se inicialização for bem-sucedida, False caso contrário
        """
        if self.initialized:
            return True

        if self.simulated_mode:
            logger.info("Serviço Oracle inicializado em modo simulado")
            self.initialized = True
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

            # Testa a conexão
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()

            self.initialized = True
            logger.info("Serviço Oracle inicializado com sucesso")
            return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao inicializar Oracle: {error_obj.message}")
            return False

    def get_connection_info(self) -> Dict[str, str]:
        """
        Obtém informações detalhadas sobre a conexão Oracle.

        Returns:
            Dict: Informações da conexão Oracle
        """
        if self.simulated_mode or not self.connection:
            return {
                "version": "Oracle Database Simulado",
                "instance": "SIMULATED",
                "server": self.host
            }

        try:
            cursor = self.connection.cursor()

            # Versão do banco
            cursor.execute("""
                SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'
            """)
            version = cursor.fetchone()[0]

            # Nome da instância
            cursor.execute("SELECT instance_name FROM v$instance")
            instance = cursor.fetchone()[0]

            cursor.close()

            return {
                "version": version,
                "instance": instance,
                "server": self.host
            }

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao obter informações da conexão: {error_obj.message}")
            return {
                "version": "Desconhecida",
                "instance": "Desconhecida",
                "server": self.host,
                "error": error_obj.message
            }

    def is_healthy(self) -> bool:
        """
        Verifica se a conexão com Oracle está saudável.

        Returns:
            bool: True se conexão estiver operacional, False caso contrário
        """
        if self.simulated_mode:
            return True

        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            return result[0] == 1
        except cx_Oracle.Error:
            return False

    def register_session(self, session_id: str) -> bool:
        """
        Registra uma sessão de colheita no Oracle.

        Args:
            session_id: Identificador único da sessão

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if not self.initialized and not self.initialize():
            return False

        if self.simulated_mode:
            logger.info(f"Registrando sessão {session_id} no Oracle (simulado)")
            return True

        try:
            cursor = self.connection.cursor()

            # Verifica se sessão já existe
            cursor.execute(
                "SELECT COUNT(*) FROM sessions WHERE session_id = :session_id",
                session_id=session_id
            )

            if cursor.fetchone()[0] > 0:
                logger.warning(f"Sessão {session_id} já pode estar registrada")
                cursor.close()
                return False

            # Insere nova sessão
            cursor.execute("""
                INSERT INTO sessions (
                    session_id, start_timestamp, status, created_by, last_updated, version
                ) VALUES (
                    :session_id, :start_timestamp, :status, :created_by,
                    :last_updated, 1
                )
            """,
                session_id=session_id,
                start_timestamp=datetime.now(),
                status='active',
                created_by='system',
                last_updated=datetime.now()
            )

            self.connection.commit()
            cursor.close()

            logger.info(f"Sessão {session_id} registrada com sucesso")
            return True

        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao iniciar sessão: {error_obj.message}")
            if hasattr(error_obj, 'code'):
                logger.error(f"Help: https://docs.oracle.com/error-help/db/ora-{error_obj.code:05d}/")
            self.connection.rollback()
            return False

    def save_sensor_data(self, session_id: str, filepath: str) -> bool:
        """
        Salva dados de sensores no Oracle a partir de um arquivo JSON.

        Args:
            session_id: Identificador da sessão
            filepath: Caminho para o arquivo JSON com dados

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if not self.initialized and not self.initialize():
            return False

        try:
            # Carrega dados do arquivo
            with open(filepath, 'r') as f:
                file_data = json.load(f)

            # Verifica se estrutura é válida
            if 'data' not in file_data:
                logger.warning(f"Estrutura de dados inválida no arquivo: {filepath}")
                return False

            sensor_data = file_data['data']
            timestamp = self._parse_timestamp(file_data.get('timestamp'))

            if self.simulated_mode:
                logger.info(f"Dados de sensores salvos: {os.path.basename(filepath)} (simulado)")
                return True

            # Processa dados para o Oracle
            processed_data = []

            # Processa cada sensor
            for sensor_name, reading in sensor_data.items():
                # Ignora campos não-sensor como timestamp global
                if sensor_name in ['timestamp', 'is_raining']:
                    continue

                sensor_value = None
                sensor_unit = ''
                sensor_timestamp = timestamp

                # Extrai dados conforme formato
                if isinstance(reading, dict) and 'value' in reading:
                    sensor_value = reading.get('value')
                    sensor_unit = reading.get('unit', '')
                    if 'timestamp' in reading:
                        sensor_timestamp = self._parse_timestamp(reading['timestamp'])
                else:
                    sensor_value = reading

                # Valida valor numérico
                try:
                    validated_value = self._validate_number(sensor_value)

                    processed_data.append({
                        'session_id': session_id,
                        'timestamp': sensor_timestamp,
                        'sensor_type': self._validate_string(sensor_name, 30),
                        'sensor_value': validated_value,
                        'unit': self._validate_string(sensor_unit, 10),
                        'quality_flag': 'GOOD'
                    })
                except (ValueError, TypeError, InvalidOperation) as e:
                    logger.warning(
                        f"Valor inválido para sensor {sensor_name}: {sensor_value}. "
                        f"Erro: {str(e)}"
                    )
                    # Continua processando outros sensores

            # Se não houver dados válidos após processamento
            if not processed_data:
                logger.warning(f"Nenhum dado válido para salvar em: {filepath}")
                return False

            # Salva dados no Oracle
            cursor = self.connection.cursor()

            # Prepara SQL
            sql = """
                INSERT INTO sensor_data (
                    session_id, timestamp, sensor_type,
                    sensor_value, unit, quality_flag
                ) VALUES (
                    :session_id, :timestamp, :sensor_type,
                    :sensor_value, :unit, :quality_flag
                )
            """

            # Executa inserção em lote
            cursor.executemany(sql, processed_data)

            self.connection.commit()
            cursor.close()

            logger.info(f"Dados de sensores salvos: {os.path.basename(filepath)}")
            return True

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao ler arquivo {filepath}: {str(e)}")
            return False
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de sensores: {error_obj.message}")
            if hasattr(error_obj, 'code'):
                logger.error(f"Help: https://docs.oracle.com/error-help/db/ora-{error_obj.code:05d}/")
            logger.warning(f"Falha ao salvar dados: {os.path.basename(filepath)}")
            if self.connection:
                self.connection.rollback()
            return False

    def save_analysis_data(self, session_id: str, filepath: str) -> bool:
        """
        Salva dados de análise no Oracle a partir de um arquivo JSON.

        Args:
            session_id: Identificador da sessão
            filepath: Caminho para o arquivo JSON com análise

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if not self.initialized and not self.initialize():
            return False

        try:
            # Carrega dados do arquivo
            with open(filepath, 'r') as f:
                file_data = json.load(f)

            # Verifica se estrutura é válida
            if 'analysis' not in file_data:
                logger.warning(f"Estrutura de análise inválida no arquivo: {filepath}")
                return False

            analysis_data = file_data['analysis']
            timestamp = self._parse_timestamp(file_data.get('timestamp'))

            if self.simulated_mode:
                logger.info(f"Dados de análise salvos: {os.path.basename(filepath)} (simulado)")
                return True

            # Extrai dados relevantes para Oracle
            loss_estimate = self._validate_number(analysis_data.get('loss_estimate', 0))

            # Converte lista de fatores para string JSON
            factors_data = analysis_data.get('problematic_factors', [])
            factors_json = json.dumps(factors_data)

            # Limita tamanho do campo
            if len(factors_json) > self.max_string_length:
                # Reduz para ajustar ao limite
                factors_json = json.dumps(factors_data[:5])

            # Extrai categorias de perda
            loss_category = self._validate_string(
                analysis_data.get('loss_category', 'unknown'),
                10
            )

            # Salva dados no Oracle
            cursor = self.connection.cursor()

            # Prepara SQL
            sql = """
                INSERT INTO harvest_losses (
                    session_id, timestamp, loss_percent, factors,
                    confidence_level, field_conditions
                ) VALUES (
                    :session_id, :timestamp, :loss_percent, :factors,
                    :confidence_level, :field_conditions
                )
            """

            cursor.execute(
                sql,
                session_id=session_id,
                timestamp=timestamp,
                loss_percent=loss_estimate,
                factors=factors_json,
                confidence_level=loss_category,
                field_conditions=None  # Não disponível nos dados analisados
            )

            self.connection.commit()
            cursor.close()

            logger.info(f"Dados de análise salvos: {os.path.basename(filepath)}")
            return True

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao ler arquivo {filepath}: {str(e)}")
            return False
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de análise: {error_obj.message}")
            if hasattr(error_obj, 'code'):
                logger.error(f"Help: https://docs.oracle.com/error-help/db/ora-{error_obj.code:05d}/")
            if self.connection:
                self.connection.rollback()
            return False

    def save_emission_data(self, session_id: str, filepath: str) -> bool:
        """
        Salva dados de emissões GHG no Oracle a partir de um arquivo JSON.

        Args:
            session_id: Identificador da sessão
            filepath: Caminho para o arquivo JSON com emissões

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if not self.initialized and not self.initialize():
            return False

        try:
            # Carrega dados do arquivo
            with open(filepath, 'r') as f:
                file_data = json.load(f)

            # Verifica se estrutura é válida
            if 'inventory' not in file_data:
                logger.warning(f"Estrutura de emissões inválida no arquivo: {filepath}")
                return False

            emissions_data = file_data['inventory']
            timestamp = self._parse_timestamp(file_data.get('timestamp'))

            if self.simulated_mode:
                logger.info(f"Dados de emissões salvos: {os.path.basename(filepath)} (simulado)")
                return True

            # Processa dados para o Oracle
            processed_emissions = []

            # Extrai dados de emissões por escopo
            for scope_name, scope_data in emissions_data.items():
                # Só processa escopos de emissão
                if not scope_name.startswith('scope'):
                    continue

                # Extrai número do escopo
                scope_match = re.match(r'scope(\d)', scope_name)
                if not scope_match:
                    continue

                scope_num = int(scope_match.group(1))

                # Processa diferentes formatos de escopo
                if scope_num == 1:
                    # Escopo 1 tem categorias adicionais
                    for category, category_data in scope_data.items():
                        for source, source_data in category_data.items():
                            for gas, value in source_data.items():
                                if gas in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                    try:
                                        processed_emissions.append({
                                            'session_id': session_id,
                                            'timestamp': timestamp,
                                            'scope': scope_num,
                                            'category': self._validate_string(category, 30),
                                            'source': self._validate_string(source, 50),
                                            'gas': gas,
                                            'value': self._validate_number(value),
                                            'unit': 'kg',
                                            'calculation_method': 'tier1',
                                            'uncertainty_percent': 10.0
                                        })
                                    except (ValueError, TypeError) as e:
                                        logger.warning(
                                            f"Valor inválido para emissão {gas} em {source}: {value}. "
                                            f"Erro: {str(e)}"
                                        )
                else:
                    # Escopos 2 e 3 são mais simples
                    for source, source_data in scope_data.items():
                        for gas, value in source_data.items():
                            if gas in ['CO2', 'CH4', 'N2O', 'CO2e']:
                                try:
                                    processed_emissions.append({
                                        'session_id': session_id,
                                        'timestamp': timestamp,
                                        'scope': scope_num,
                                        'category': '',  # Não tem categoria
                                        'source': self._validate_string(source, 50),
                                        'gas': gas,
                                        'value': self._validate_number(value),
                                        'unit': 'kg',
                                        'calculation_method': 'tier1',
                                        'uncertainty_percent': 10.0
                                    })
                                except (ValueError, TypeError) as e:
                                    logger.warning(
                                        f"Valor inválido para emissão {gas} em {source}: {value}. "
                                        f"Erro: {str(e)}"
                                    )

            # Se não houver dados válidos após processamento
            if not processed_emissions:
                logger.warning(f"Nenhum dado de emissão válido para salvar em: {filepath}")
                return False

            # Salva dados no Oracle
            cursor = self.connection.cursor()

            # Prepara SQL
            sql = """
                INSERT INTO ghg_emissions (
                    session_id, timestamp, scope, category, source,
                    gas, value, unit, calculation_method, uncertainty_percent
                ) VALUES (
                    :session_id, :timestamp, :scope, :category, :source,
                    :gas, :value, :unit, :calculation_method, :uncertainty_percent
                )
            """

            # Executa inserção em lote
            cursor.executemany(sql, processed_emissions)

            self.connection.commit()
            cursor.close()

            logger.info(f"Dados de emissões salvos: {os.path.basename(filepath)}")
            return True

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao ler arquivo {filepath}: {str(e)}")
            return False
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de emissões: {error_obj.message}")
            if hasattr(error_obj, 'code'):
                logger.error(f"Help: https://docs.oracle.com/error-help/db/ora-{error_obj.code:05d}/")
            if self.connection:
                self.connection.rollback()
            return False

    def save_carbon_stock_data(self, session_id: str, filepath: str) -> bool:
        """
        Salva dados de estoques de carbono no Oracle a partir de um arquivo JSON.

        Args:
            session_id: Identificador da sessão
            filepath: Caminho para o arquivo JSON com dados

        Returns:
            bool: True se operação bem-sucedida, False caso contrário
        """
        if not self.initialized and not self.initialize():
            return False

        try:
            # Carrega dados do arquivo
            with open(filepath, 'r') as f:
                file_data = json.load(f)

            # Verifica se estrutura é válida
            if 'carbon_stocks' not in file_data:
                logger.warning(f"Estrutura de estoque inválida no arquivo: {filepath}")
                return False

            stock_data = file_data['carbon_stocks']
            timestamp = self._parse_timestamp(file_data.get('timestamp'))

            if self.simulated_mode:
                logger.info(f"Dados de estoque salvos: {os.path.basename(filepath)} (simulado)")
                return True

            # Processa dados para o Oracle
            processed_stocks = []

            # Processa cada tipo de estoque
            for stock_type, stock_info in stock_data.items():
                # Só processa tipos conhecidos
                valid_types = [
                    'soil_organic_carbon', 'above_ground_biomass',
                    'below_ground_biomass', 'dead_organic_matter'
                ]

                if stock_type not in valid_types:
                    continue

                try:
                    # Extrai e valida valores
                    change = self._validate_number(stock_info.get('change_co2', 0))
                    amort_period = int(stock_info.get('amortization_period', 20))
                    method = self._validate_string(
                        stock_info.get('measurement_method', 'model_estimate'),
                        30
                    )

                    processed_stocks.append({
                        'session_id': session_id,
                        'timestamp': timestamp,
                        'stock_type': stock_type,
                        'change': change,
                        'amortization_period': amort_period,
                        'unit': 'kg CO2',
                        'measurement_method': method
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Valor inválido para estoque {stock_type}: {stock_info}. "
                        f"Erro: {str(e)}"
                    )

            # Se não houver dados válidos após processamento
            if not processed_stocks:
                logger.warning(f"Nenhum dado de estoque válido para salvar em: {filepath}")
                return False

            # Salva dados no Oracle
            cursor = self.connection.cursor()

            # Prepara SQL
            sql = """
                INSERT INTO carbon_stocks (
                    session_id, timestamp, stock_type, change,
                    amortization_period, unit, measurement_method
                ) VALUES (
                    :session_id, :timestamp, :stock_type, :change,
                    :amortization_period, :unit, :measurement_method
                )
            """

            # Executa inserção em lote
            cursor.executemany(sql, processed_stocks)

            self.connection.commit()
            cursor.close()

            logger.info(f"Dados de estoque salvos: {os.path.basename(filepath)}")
            return True

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Erro ao ler arquivo {filepath}: {str(e)}")
            return False
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao salvar dados de estoque: {error_obj.message}")
            if hasattr(error_obj, 'code'):
                logger.error(f"Help: https://docs.oracle.com/error-help/db/ora-{error_obj.code:05d}/")
            if self.connection:
                self.connection.rollback()
            return False

    def close(self) -> bool:
        """
        Fecha conexão com o banco Oracle.

        Returns:
            bool: True se fechamento for bem-sucedido, False caso contrário
        """
        if self.simulated_mode or not self.connection:
            self.initialized = False
            return True

        try:
            self.connection.close()
            self.connection = None
            self.initialized = False
            logger.info("Conexão Oracle fechada com sucesso")
            return True
        except cx_Oracle.Error as e:
            error_obj, = e.args
            logger.error(f"Erro ao fechar conexão Oracle: {error_obj.message}")
            return False

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> datetime:
        """
        Converte string de timestamp para objeto datetime.

        Args:
            timestamp_str: String com timestamp ISO ou similar

        Returns:
            datetime: Objeto datetime correspondente ou datetime atual
        """
        if not timestamp_str:
            return datetime.now()

        try:
            # Tenta formato ISO completo
            return datetime.fromisoformat(timestamp_str)
        except (ValueError, TypeError):
            try:
                # Tenta formato alternativo
                return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                # Retorna data/hora atual em caso de falha
                return datetime.now()

    def _validate_number(self, value: Any) -> float:
        """
        Valida e converte valor para formato numérico aceitável pelo Oracle.

        Args:
            value: Valor a ser validado e convertido

        Returns:
            float: Valor numérico validado

        Raises:
            ValueError: Se valor não puder ser convertido para número
        """
        if value is None:
            raise ValueError("Valor não pode ser None")

        if isinstance(value, bool):
            raise ValueError("Valor booleano não pode ser convertido para número")

        # Tentativa de conversão através de Decimal para maior precisão
        if isinstance(value, str):
            # Remove caracteres não numéricos exceto ponto e sinal
            value = re.sub(r'[^\d.-]', '', value)

        try:
            # Converte para Decimal e depois para float para evitar problemas de precisão
            dec_value = Decimal(str(value))
            return float(dec_value)
        except (InvalidOperation, ValueError, TypeError) as e:
            raise ValueError(f"Não foi possível converter para número: {value}") from e

    def _validate_string(self, value: Any, max_length: int) -> str:
        """
        Valida e trunca string para comprimento máximo aceitável pelo Oracle.

        Args:
            value: Valor a ser validado
            max_length: Comprimento máximo permitido

        Returns:
            str: String validada e truncada se necessário
        """
        if value is None:
            return ""

        # Converte para string
        str_value = str(value)

        # Trunca se necessário
        if len(str_value) > max_length:
            return str_value[:max_length]

        return str_value

    def export_session_data(self, session_id: str, data_path: str) -> Dict[str, Any]:
        """
        Exporta todos os dados de uma sessão para o Oracle.

        Args:
            session_id: Identificador da sessão
            data_path: Caminho base para os diretórios de dados

        Returns:
            Dict: Resumo da exportação com contadores e status
        """
        if not self.initialized and not self.initialize():
            logger.error("Serviço Oracle não inicializado")
            return {
                "success": False,
                "error": "Serviço Oracle não inicializado",
                "counts": {
                    "sessions": 0,
                    "sensor_data": 0,
                    "analysis": 0,
                    "emissions": 0,
                    "carbon_stocks": 0
                },
                "errors": []
            }

        # Resultados da exportação
        results = {
            "success": True,
            "counts": {
                "sessions": 0,
                "sensor_data": 0,
                "analysis": 0,
                "emissions": 0,
                "carbon_stocks": 0
            },
            "errors": []
        }

        # Registra sessão
        session_registered = self.register_session(session_id)
        if session_registered:
            results["counts"]["sessions"] = 1

        # Diretórios de dados
        dirs = {
            "sensor_data": os.path.join(data_path, "sensor_data"),
            "analysis": os.path.join(data_path, "analysis"),
            "ghg_inventory": os.path.join(data_path, "ghg_inventory"),
            "carbon_stocks": os.path.join(data_path, "carbon_stocks")
        }

        # Exporta dados de sensores
        if os.path.exists(dirs["sensor_data"]):
            for filename in os.listdir(dirs["sensor_data"]):
                if filename.startswith(f"{session_id}-") and filename.endswith('.json'):
                    filepath = os.path.join(dirs["sensor_data"], filename)
                    success = self.save_sensor_data(session_id, filepath)

                    if success:
                        results["counts"]["sensor_data"] += 1
                    else:
                        results["errors"].append(
                            f"Falha ao salvar dados de sensores: {filename}"
                        )

        # Exporta dados de análise
        if os.path.exists(dirs["analysis"]):
            for filename in os.listdir(dirs["analysis"]):
                if filename.startswith(f"{session_id}-") and filename.endswith('.json'):
                    filepath = os.path.join(dirs["analysis"], filename)
                    success = self.save_analysis_data(session_id, filepath)

                    if success:
                        results["counts"]["analysis"] += 1
                    else:
                        results["errors"].append(
                            f"Falha ao salvar dados de análise: {filename}"
                        )

        # Exporta dados de emissões
        if os.path.exists(dirs["ghg_inventory"]):
            for filename in os.listdir(dirs["ghg_inventory"]):
                if filename.startswith(f"{session_id}-") and filename.endswith('.json'):
                    filepath = os.path.join(dirs["ghg_inventory"], filename)
                    success = self.save_emission_data(session_id, filepath)

                    if success:
                        results["counts"]["emissions"] += 1
                    else:
                        results["errors"].append(
                            f"Falha ao salvar dados de emissões: {filename}"
                        )

        # Exporta dados de estoque de carbono
        if os.path.exists(dirs["carbon_stocks"]):
            for filename in os.listdir(dirs["carbon_stocks"]):
                if filename.startswith(f"{session_id}-") and filename.endswith('.json'):
                    filepath = os.path.join(dirs["carbon_stocks"], filename)
                    success = self.save_carbon_stock_data(session_id, filepath)

                    if success:
                        results["counts"]["carbon_stocks"] += 1
                    else:
                        results["errors"].append(
                            f"Falha ao salvar dados de estoque: {filename}"
                        )

        # Determina sucesso geral
        if results["errors"]:
            results["success"] = False

        return results
