#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class BoundaryManager:
    """
    Gerencia limites organizacionais e operacionais do inventário GHG.
    """

    def __init__(self, config):
        """
        Inicializa o gerenciador de limites.

        Args:
            config (dict): Configurações de limites
        """
        self.config = config
        self.org_boundary_approach = self.config.get(
            'organizational_boundary', 'operational_control'
        )
        self.scope1_enabled = True  # Sempre requerido pelo GHG Protocol
        self.scope2_enabled = True  # Sempre requerido pelo GHG Protocol
        self.scope3_enabled = self.config.get('include_scope3', False)
        self.base_period = self.config.get('base_period', '2025')

    def is_source_included(self, source, scope):
        """
        Verifica se uma fonte de emissão está dentro dos limites.

        Args:
            source (str): Nome da fonte de emissão
            scope (int): Escopo da fonte (1, 2 ou 3)

        Returns:
            bool: Indica se a fonte está incluída no inventário
        """
        if scope == 1 and self.scope1_enabled:
            return True
        if scope == 2 and self.scope2_enabled:
            return True
        if scope == 3 and self.scope3_enabled:
            # Verifica se a fonte específica de escopo 3 está habilitada
            scope3_sources = self.config.get('scope3_sources', [])
            return source in scope3_sources

        return False

    def get_operational_control_entities(self):
        """
        Retorna entidades sob controle operacional.

        Returns:
            list: Entidades dentro do limite organizacional
        """
        if self.org_boundary_approach == 'operational_control':
            return self.config.get('controlled_entities', [])
        return []

    def get_financial_control_entities(self):
        """
        Retorna entidades sob controle financeiro.

        Returns:
            list: Entidades dentro do limite organizacional
        """
        if self.org_boundary_approach == 'financial_control':
            return self.config.get('controlled_entities', [])
        return []

    def get_equity_share_entities(self):
        """
        Retorna entidades com participação acionária.

        Returns:
            dict: Entidades e percentuais de participação
        """
        if self.org_boundary_approach == 'equity_share':
            return self.config.get('equity_entities', {})
        return {}
