# Especificação Técnica: Conector Oracle para Sistema de Monitoramento GHG na Colheita de Cana-de-Açúcar

## 1. Visão Geral

Esta especificação define melhorias para o componente `OracleConnector`, responsável pela persistência de dados de monitoramento de perdas na colheita de cana-de-açúcar e emissões de GHG em um banco de dados Oracle. A implementação atual funciona em ambiente de desenvolvimento, mas requer aprimoramentos para garantir robustez, segurança e desempenho em ambiente de produção.

## 2. Diagnóstico da Implementação Atual

A classe `OracleConnector` existente apresenta os seguintes pontos a serem aprimorados:

1. **Segurança inadequada**:
   - Credenciais expostas no código
   - Vulnerabilidade a injeção SQL
   - Ausência de criptografia para dados sensíveis

2. **Tratamento limitado de erros e exceções**:
   - Falha em distinguir diferentes tipos de erros Oracle
   - Ausência de mecanismos de retry e timeout
   - Logs insuficientes para diagnóstico de falhas

3. **Problemas de desempenho**:
   - Ausência de pool de conexões
   - Ineficiência em operações de inserção em massa
   - Falta de otimização para consultas

4. **Problemas de manutenibilidade**:
   - Classe monolítica com múltiplas responsabilidades
   - Ausência de interfaces para desacoplamento
   - Documentação insuficiente

## 3. Requisitos da Nova Implementação

### 3.1 Requisitos Funcionais

1. Manter compatibilidade com as funcionalidades atuais:
   - Conexão e desconexão com Oracle
   - Criação de tabelas e esquemas
   - Gestão de sessões (início e término)
   - Persistência de dados de sensores, emissões GHG, estoques de carbono e perdas

2. Adicionar novas funcionalidades:
   - Suporte a operações em lote (batch)
   - Recuperação eficiente de dados históricos
   - Validação de dados antes da persistência
   - Mecanismos de migração de esquema

### 3.2 Requisitos Não-Funcionais

1. **Segurança**:
   - Criptografia de credenciais em armazenamento externo
   - Proteção contra injeção SQL
   - Registro de auditoria para operações críticas

2. **Desempenho**:
   - Utilização de connection pooling
   - Suporte a operações em lote
   - Otimização para grande volume de dados

3. **Confiabilidade**:
   - Mecanismos de retry automático
   - Timeouts configuráveis
   - Tratamento robusto de erros

4. **Testabilidade**:
   - Interfaces para facilitar mocks em testes
   - Separação clara de responsabilidades
   - Testes unitários abrangentes

5. **Operacionalidade**:
   - Logging detalhado
   - Métricas de performance
   - Integração com sistemas de monitoramento

## 4. Arquitetura Proposta

### 4.1 Estrutura do Pacote Oracle

Reorganizar o código em um subpacote com múltiplas classes para facilitar manutenção:

```
src/persistence/oracle/
├── __init__.py
├── connector.py         # Gerencia conexão com Oracle
├── session_dao.py       # Operações CRUD para sessões
├── sensor_dao.py        # Operações CRUD para dados de sensores
├── emissions_dao.py     # Operações CRUD para emissões GHG
├── carbon_stock_dao.py  # Operações CRUD para estoques de carbono
├── harvest_dao.py       # Operações CRUD para dados de colheita
├── config.py            # Gerencia configuração
├── schema.py            # Define e migra esquemas
└── error_handler.py     # Tratamento de erros específicos do Oracle
```

### 4.2 Padrões de Design

1. **Data Access Object (DAO)**: Encapsular operações CRUD específicas em classes separadas.

2. **Factory**: Criar e gerenciar objetos DAO através de uma fábrica central.

3. **Singleton**: Garantir única instância de pool de conexões.

4. **Strategy**: Permitir diferentes estratégias de connection pooling e retry.

5. **Repository**: Abstrair operações de persistência.

## 5. Implementação da Classe Principal

```python
# arquivo: cana-loss-reduction/src/persistence/oracle/connector.py
```

## 6. Testes Unitários

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Testes unitários para OracleConnector.

Este módulo contém testes abrangentes para validar o funcionamento
do conector Oracle em diferentes cenários e condições.
"""

import unittest
from unittest.mock import patch, MagicMock, ANY
import cx_Oracle
from datetime import datetime
import os

from src.persistence.oracle.connector import OracleConnector

class TestOracleConnector(unittest.TestCase):
    """
    Testes para o conector Oracle.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        # Configuração básica para testes
        self.test_config = {
            'connection': {
                'host': 'test-host',
                'port': 1521,
                'service_name': 'TEST',
                'username': 'test_user',
                'password': 'test_pass'
            },
            'pool': {
                'min': 1,
                'max': 5,
                'increment': 1
            },
            'retry': {
                'max_attempts': 3,
                'delay_seconds': 0.1,
                'backoff_factor': 2
            }
        }

        # Limpa variáveis de ambiente que possam interferir
        for var in ['ORACLE_USERNAME', 'ORACLE_PASSWORD']:
            if var in os.environ:
                del os.environ[var]

    @patch('cx_Oracle.SessionPool')
    @patch('cx_Oracle.makedsn')
    def test_initialize_success(self, mock_makedsn, mock_session_pool):
        """
        Testa inicialização bem-sucedida do pool.
        """
        # Configura mocks
        mock_makedsn.return_value = "test-dsn"
        mock_pool = MagicMock()
        mock_cursor = MagicMock()
        mock_conn = MagicMock()

        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.acquire.return_value = mock_conn
        mock_session_pool.return_value = mock_pool

        # Testa inicialização
        connector = OracleConnector(self.test_config)
        result = connector.initialize()

        # Verifica resultados
        self.assertTrue(result)
        self.assertTrue(connector.initialized)
        mock_makedsn.assert_called_once_with(
            'test-host', 1521, service_name='TEST'
        )
        mock_session_pool.assert_called_once_with(
            user='test_user',
            password='test_pass',
            dsn='test-dsn',
            min=1,
            max=5,
            increment=1,
            threaded=True,
            getmode=ANY
        )

    @patch('cx_Oracle.SessionPool')
    @patch('cx_Oracle.makedsn')
    def test_initialize_failure(self, mock_makedsn, mock_session_pool):
        """
        Testa falha na inicialização do pool.
        """
        # Configura mock para falhar
        mock_makedsn.return_value = "test-dsn"
        mock_session_pool.side_effect = cx_Oracle.Error()

        # Testa inicialização com falha
        connector = OracleConnector(self.test_config)
        result = connector.initialize()

        # Verifica resultados
        self.assertFalse(result)
        self.assertFalse(connector.initialized)

    def test_simulated_mode(self):
        """
        Testa funcionamento em modo simulado.
        """
        # Configura conector em modo simulado
        config = self.test_config.copy()
        config['simulated_mode'] = True
        connector = OracleConnector(config)

        # Verifica inicialização
        self.assertTrue(connector.initialize())
        self.assertTrue(connector.initialized)

        # Verifica outros métodos em modo simulado
        self.assertTrue(connector.create_tables())
        self.assertTrue(connector.start_session("test-session"))
        self.assertTrue(connector.end_session("test-session"))
        self.assertTrue(connector.save_sensor_data("test-session", {"temp": 25.0}))
        self.assertTrue(connector.shutdown())

    @patch('cx_Oracle.SessionPool')
    @patch('cx_Oracle.makedsn')
    def test_environment_credentials(self, mock_makedsn, mock_session_pool):
        """
        Testa uso de credenciais via variáveis de ambiente.
        """
        # Configura variáveis de ambiente
        os.environ['ORACLE_USERNAME'] = 'env_user'
        os.environ['ORACLE_PASSWORD'] = 'env_pass'

        # Configura mocks
        mock_makedsn.return_value = "test-dsn"
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_pool.acquire.return_value = mock_conn
        mock_session_pool.return_value = mock_pool

        # Testa inicialização
        connector = OracleConnector(self.test_config)
        connector.initialize()

        # Verifica uso das credenciais das variáveis de ambiente
        mock_session_pool.assert_called_once_with(
            user='env_user',
            password='env_pass',
            dsn='test-dsn',
            min=1,
            max=5,
            increment=1,
            threaded=True,
            getmode=ANY
        )

        # Limpa variáveis de ambiente
        del os.environ['ORACLE_USERNAME']
        del os.environ['ORACLE_PASSWORD']

    # Testes adicionais para os outros métodos seguiriam o mesmo padrão
```

## 7. Considerações de Implementação

### 7.1 Segurança

1. **Gerenciamento de Credenciais**:
   - Priorizar o uso de variáveis de ambiente para credenciais
   - Implementar alerta se credenciais são usadas diretamente da configuração
   - Não logar credenciais em logs

2. **Proteção contra Injeção SQL**:
   - Usar exclusivamente prepared statements e parâmetros nomeados
   - Implementar validação e sanitização de inputs

3. **Auditoria**:
   - Adicionar colunas de auditoria (created_by, last_updated, version)
   - Implementar optimistic locking para evitar conflitos de atualização

### 7.2 Desempenho

1. **Connection Pooling**:
   - Implementar pool de conexões com min/max configurável
   - Configurar timeout para conexões ociosas

2. **Operações em Lote**:
   - Utilizar `executemany()` para inserções múltiplas
   - Implementar transaction batching para grandes volumes

3. **Índices e Otimização**:
   - Criar índices apropriados para consultas frequentes
   - Utilizar hints Oracle em consultas críticas

### 7.3 Resiliência

1. **Mecanismos de Retry**:
   - Implementar backoff exponencial para tratamento de falhas temporárias
   - Limitar número máximo de retries

2. **Tratamento de Erros**:
   - Classificar erros entre recuperáveis e não-recuperáveis
   - Distinguir erros de dados vs. erros de infraestrutura

3. **Transações e Isolamento**:
   - Garantir atomicidade de operações críticas
   - Implementar compensating transactions quando necessário

## 8. Conclusão

O conector Oracle proposto representa uma evolução significativa em relação à implementação atual, fornecendo maior robustez, segurança e desempenho necessários para um ambiente de produção. A implementação mantém a compatibilidade com o código existente enquanto introduz melhorias fundamentais:

1. **Abordagem production-ready** com connection pooling, retry e timeouts
2. **Segurança aprimorada** com manejo adequado de credenciais e proteção contra injeção SQL
3. **Desempenho otimizado** com operações em lote e índices apropriados
4. **Alta testabilidade** com separação clara de responsabilidades
5. **Observabilidade** com logs detalhados

Esta implementação é adequada para o contexto de monitoramento de GHG na colheita de cana-de-açúcar, onde confiabilidade e desempenho são essenciais para garantir a integridade dos dados coletados e processados.