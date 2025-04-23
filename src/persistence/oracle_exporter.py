# src/persistence/oracle_exporter.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Implementa funcionalidade de exportação de dados para Oracle.

Este módulo fornece uma interface para exportar dados do sistema
para o banco de dados Oracle, com tratamento de erros e feedback
detalhado sobre o processo.
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List

from persistence.oracle.oracle_service import OracleService

class OracleExporter:
    """
    Exporta dados de colheita para o banco de dados Oracle.

    Gerencia o processo de exportação de dados coletados durante a
    simulação de colheita para persistência permanente no Oracle,
    incluindo relatórios de progresso e tratamento de erros.
    """

    def __init__(self, config: Dict[str, Any] = None, data_path: str = "data"):
        """
        Inicializa o exportador Oracle.

        Args:
            config: Configurações do serviço Oracle
            data_path: Caminho base para diretórios de dados
        """
        self.config = config or {}
        self.data_path = data_path
        self.service = OracleService(config)

    def export_session(self, session_id: str) -> Dict[str, Any]:
        """
        Exporta todos os dados de uma sessão para o Oracle.

        Args:
            session_id: Identificador da sessão

        Returns:
            Dict: Resultado da exportação com estatísticas e erros
        """
        # Inicializa serviço Oracle
        print("\nInicializando serviço Oracle...")
        if not self.service.initialize():
            return {
                "success": False,
                "error": "Falha ao inicializar serviço Oracle",
                "counts": {
                    "sessions": 0,
                    "sensor_data": 0,
                    "analysis": 0,
                    "emissions": 0,
                    "carbon_stocks": 0
                },
                "errors": ["Falha ao inicializar serviço Oracle"]
            }

        # Verifica saúde da conexão
        print("Verificando saúde da conexão...\n")
        if not self.service.is_healthy():
            return {
                "success": False,
                "error": "Conexão Oracle não está operacional",
                "counts": {
                    "sessions": 0,
                    "sensor_data": 0,
                    "analysis": 0,
                    "emissions": 0,
                    "carbon_stocks": 0
                },
                "errors": ["Conexão Oracle não está operacional"]
            }

        # Exibe informações da conexão
        conn_info = self.service.get_connection_info()
        print("Conectado ao banco Oracle:")
        print(f"• Versão: {conn_info.get('version', 'Desconhecida')}")
        print(f"• Instância: {conn_info.get('instance', 'Desconhecida')}")
        print(f"• Servidor: {conn_info.get('server', 'Desconhecido')}")

        # Exporta dados
        print("\nExportando dados para o Oracle...")
        print("Este processo pode levar alguns minutos dependendo da")
        print("quantidade de dados e da velocidade da conexão.\n")

        print("Processando...\n")
        time.sleep(1)  # Pausa para legibilidade da UI

        # Executa exportação
        result = self.service.export_session_data(session_id, self.data_path)

        # Fecha conexão
        self.service.close()

        return result

    def format_export_summary(self, result: Dict[str, Any]) -> str:
        """
        Formata o resumo da exportação para exibição.

        Args:
            result: Resultado da exportação

        Returns:
            str: Resumo formatado para exibição
        """
        counts = result.get("counts", {})
        errors = result.get("errors", [])

        summary = []

        # Título baseado no resultado
        if result.get("success", False):
            summary.append("-" * 80)
            summary.append("EXPORTAÇÃO CONCLUÍDA COM SUCESSO".center(80))
            summary.append("-" * 80)
        else:
            if errors:
                summary.append("-" * 80)
                summary.append("EXPORTAÇÃO CONCLUÍDA COM AVISOS".center(80))
                summary.append("-" * 80)
            else:
                summary.append("-" * 80)
                summary.append("EXPORTAÇÃO FALHOU".center(80))
                summary.append("-" * 80)

        # Dados exportados
        summary.append("Dados exportados:")
        summary.append(f"• Sessões: {counts.get('sessions', 0)}")
        summary.append(f"• Dados de sensores: {counts.get('sensor_data', 0)} arquivos")
        summary.append(f"• Análises de perdas: {counts.get('analysis', 0)} arquivos")
        summary.append(f"• Dados de emissões: {counts.get('emissions', 0)} arquivos")
        summary.append(f"• Estoques de carbono: {counts.get('carbon_stocks', 0)} arquivos")

        # Erros ocorridos
        if errors:
            summary.append(f"\nOcorreram {len(errors)} erros durante a exportação:")

            # Limita a exibição de erros para não sobrecarregar a tela
            max_errors = 5
            for i, error in enumerate(errors[:max_errors], 1):
                summary.append(f"{i}. {error}")

            if len(errors) > max_errors:
                summary.append(f"...e mais {len(errors) - max_errors} erros não exibidos.")

            summary.append("\nAlguns dados foram exportados com sucesso, mas ocorreram erros.")
            summary.append("Verifique os logs para mais detalhes.")
        elif not result.get("success", False):
            # Falha sem erros específicos
            summary.append("\nA exportação falhou sem erros específicos registrados.")
            summary.append("Verifique a conexão e as configurações do Oracle.")

        return "\n".join(summary)

def main():
    """
    Função principal para uso em linha de comando.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Exportador de dados para Oracle"
    )
    parser.add_argument(
        "--session",
        required=True,
        help="ID da sessão para exportar"
    )
    parser.add_argument(
        "--data-path",
        default="data",
        help="Caminho base para diretórios de dados"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host do servidor Oracle"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=1521,
        help="Porta do servidor Oracle"
    )
    parser.add_argument(
        "--service",
        default="ORCL",
        help="Nome do serviço Oracle"
    )
    parser.add_argument(
        "--user",
        default="system",
        help="Usuário Oracle"
    )
    parser.add_argument(
        "--password",
        help="Senha Oracle (se omitida, será solicitada)"
    )
    parser.add_argument(
        "--simulated",
        action="store_true",
        help="Executar em modo simulado (sem conexão real)"
    )

    args = parser.parse_args()

    # Solicita senha se não fornecida
    password = args.password
    if not password and not args.simulated:
        import getpass
        password = getpass.getpass("Senha Oracle: ")

    # Configuração
    config = {
        "host": args.host,
        "port": args.port,
        "service_name": args.service,
        "username": args.user,
        "password": password,
        "simulated_mode": args.simulated,
        "validate_data": True
    }

    # Executa exportação
    exporter = OracleExporter(config, args.data_path)
    result = exporter.export_session(args.session)

    # Exibe resumo
    print(exporter.format_export_summary(result))

    # Código de saída
    if not result.get("success", False):
        sys.exit(1)

if __name__ == "__main__":
    main()
