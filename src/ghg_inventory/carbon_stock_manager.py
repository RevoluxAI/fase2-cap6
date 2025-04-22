#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class CarbonStockManager:
    """
    Gerencia contabilização de estoques de carbono.
    """

    def __init__(self, config):
        """
        Inicializa o gerenciador de estoques de carbono.

        Args:
            config (dict): Configurações para gestão de estoques
        """
        self.config = config
        self.amortization_period = config.get('amortization_period', 20)
        self.tracked_stocks = {
            'soil_organic_carbon': config.get('track_soil_carbon', True),
            'above_ground_biomass': config.get('track_above_ground', True),
            'below_ground_biomass': config.get('track_below_ground', True),
            'dead_organic_matter': config.get('track_dom', True)
        }
        self.historical_data = {}

    def calculate_stock_changes(self, current_data, previous_data=None):
        """
        Calcula mudanças em estoques de carbono.

        Args:
            current_data (dict): Dados atuais de estoques
            previous_data (dict): Dados anteriores de estoques

        Returns:
            dict: Mudanças em estoques por categoria
        """
        changes = {}

        for stock_type, is_tracked in self.tracked_stocks.items():
            if not is_tracked:
                continue

            current_value = current_data.get(stock_type, 0)
            previous_value = 0

            if previous_data:
                previous_value = previous_data.get(stock_type, 0)

            # Mudança positiva = sequestro; negativa = emissão
            change = current_value - previous_value

            # Converte C para CO2 multiplicando por 44/12
            change_co2 = change * 44/12

            changes[stock_type] = {
                'change_c': change,
                'change_co2': change_co2,
                'amortization_period': self.amortization_period
            }

        return changes

    def calculate_amortized_flux(self, stock_changes, period=1):
        """
        Calcula fluxo amortizado para o período.

        Args:
            stock_changes (dict): Mudanças em estoques
            period (int): Período específico dentro do período total

        Returns:
            dict: Fluxo amortizado por tipo de estoque
        """
        amortized_flux = {}

        for stock_type, change_data in stock_changes.items():
            total_change = change_data.get('change_co2', 0)
            amort_period = change_data.get('amortization_period',
                                          self.amortization_period)

            # Distribuição linear sobre o período de amortização
            if amort_period > 0:
                amortized_flux[stock_type] = total_change / amort_period
            else:
                amortized_flux[stock_type] = total_change

        return amortized_flux

    def is_land_use_change(self, previous_use, current_use):
        """
        Determina se ocorreu mudança no uso da terra.

        Args:
            previous_use (str): Uso anterior da terra
            current_use (str): Uso atual da terra

        Returns:
            bool: Indica se houve mudança no uso da terra
        """
        land_use_categories = [
            'forest', 'cropland', 'grassland', 'wetland', 'settlements'
        ]

        # Considerado LUC se a categoria mudou
        return (previous_use in land_use_categories and
                current_use in land_use_categories and
                previous_use != current_use)
