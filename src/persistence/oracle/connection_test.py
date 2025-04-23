# src/persistence/oracle/connection_test.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para teste de conexão com banco de dados Oracle.

Este módulo permite testar a conexão com o banco Oracle e verificar
se os componentes de persistência estão configurados corretamente.
"""

import os
import sys
import json
import logging

# Adiciona diretório raiz ao path para importações relativas
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               '../..')))

from persistence.oracle.connector import OracleConnector
from persistence.oracle.session_dao import SessionDAO

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    """
    Carrega configuração do Oracle de arquivo ou solicita interativamente.

    Returns:
        dict: Configurações para conexão Oracle
    """
    config_path = os.path.join('data', 'config.json')

    # Tenta carregar de arquivo existente
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                if 'oracle' in config:
                    return config['oracle']
        except json.JSONDecodeError:
            logger.warning("Arquivo de configuração inválido")

    # Solicita configuração interativamente
    print("Configuração Oracle não encontrada. Informe os dados de conexão:")

    config = {
        'host': input("Host [localhost]: ") or 'localhost',
        'port': int(input("Porta [1521]: ") or '1521'),
        'service_name': input("Nome do serviço [ORCL]: ") or 'ORCL',
        'username': input("Usuário [system]: ") or 'system',
        'password': input("Senha [oracle]: ") or 'oracle',
        'simulated_mode': input("Modo simulado (s/n) [s]: ").lower() != 'n'
    }

    # Salva configuração para uso futuro
    try:
        if not os.path.exists('data'):
            os.makedirs('data')

        with open(config_path, 'w') as f:
            json.dump({'oracle': config}, f, indent=2)
    except Exception as e:
        logger.warning(f"Não foi possível salvar configuração: {str(e)}")

    return config


def test_connection(config):
    """
    Testa conexão com Oracle usando configuração fornecida.

    Args:
        config: Configurações para conexão Oracle

    Returns:
        bool: Sucesso da conexão
    """
    print("\nTESTANDO CONEXÃO ORACLE")
    print("=" * 50)

    # Exibe configuração
    print(f"Host: {config['host']}")
    print(f"Porta: {config['port']}")
    print(f"Serviço: {config['service_name']}")
    print(f"Usuário: {config['username']}")
    print(f"Modo simulado: {'Sim' if config.get('simulated_mode') else 'Não'}")
    print("-" * 50)

    # Inicializa conector
    connector = OracleConnector(config)

    # Testa inicialização
    print("Inicializando conector... ", end="")
    result = connector.initialize()
    print("OK" if result else "FALHA")

    if not result:
        print("Falha ao inicializar conector Oracle.")
        return False

    # Testa saúde da conexão
    print("Verificando saúde da conexão... ", end="")
    health = connector.is_healthy()
    print("OK" if health else "FALHA")

    if not health:
        print("Conexão não está saudável.")
        return False

    # Obtém informações do banco
    print("Obtendo informações do banco... ", end="")
    db_info = connector.get_database_info()

    if 'error' in db_info:
        print("FALHA")
        print(f"Erro: {db_info['error']}")
        return False
    else:
        print("OK")

    # Exibe informações do banco
    print("\nINFORMAÇÕES DO BANCO:")
    print(f"Versão: {db_info.get('version', 'Desconhecida')}")
    print(f"Instância: {db_info.get('instance_name', 'Desconhecida')}")
    print(f"Servidor: {db_info.get('hostname', 'Desconhecido')}")
    print(f"Banco: {db_info.get('database_name', 'Desconhecido')}")

    # Testa criação de tabelas
    print("\nCriando tabelas... ", end="")
    tables_result = connector.create_tables()
    print("OK" if tables_result else "FALHA")

    if not tables_result:
        print("Falha ao criar tabelas.")
        return False

    # Testa criação e finalização de sessão
    try:
        print("Testando DAO de sessão... ", end="")
        session_dao = SessionDAO(connector)
        session_id = session_dao.create_session({
            'created_by': 'connection_test'
        })

        # Tenta finalizar sessão
        if session_dao.end_session(session_id, 'completed'):
            print("OK")
        else:
            print("FALHA ao finalizar sessão")
            return False
    except Exception as e:
        print("FALHA")
        print(f"Erro ao testar DAO de sessão: {str(e)}")
        return False

    # Finaliza conexão
    print("Finalizando conexão... ", end="")
    shutdown_result = connector.shutdown()
    print("OK" if shutdown_result else "FALHA")

    print("\nTESTE DE CONEXÃO CONCLUÍDO COM SUCESSO!")
    return True


def main():
    """
    Função principal para teste de conexão.
    """
    print("TESTE DE CONEXÃO ORACLE")
    print("======================\n")

    try:
        # Carrega configuração
        config = load_config()

        # Testa conexão
        success = test_connection(config)

        if success:
            print("\nA conexão com o Oracle foi testada com sucesso!")
            print("Todos os componentes estão funcionando corretamente.")
        else:
            print("\nOcorreram erros durante o teste de conexão.")
            print("Verifique as mensagens acima para mais detalhes.")

    except KeyboardInterrupt:
        print("\nTeste interrompido pelo usuário.")
    except Exception as e:
        print(f"\nErro inesperado: {str(e)}")
        import traceback
        traceback.print_exc()

    print("\nPressione ENTER para sair...")
    input()


if __name__ == "__main__":
    main()