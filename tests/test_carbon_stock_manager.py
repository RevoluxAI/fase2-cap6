#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4. Testes Unitários
# tests/test_carbon_stock_manager.py
import unittest
from src.ghg_inventory.carbon_stock_manager import CarbonStockManager


class TestCarbonStockManager(unittest.TestCase):
    """
    Testes para o gerenciador de estoques de carbono.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        config = {
            'amortization_period': 20,
            'track_soil_carbon': True,
            'track_above_ground': True,
            'track_below_ground': True,
            'track_dom': True
        }
        self.manager = CarbonStockManager(config)

    def test_calculate_stock_changes(self):
        """
        Testa cálculo de mudanças em estoques de carbono.
        """
        current_data = {
            'soil_organic_carbon': 100.0,
            'above_ground_biomass': 50.0,
            'below_ground_biomass': 20.0,
            'dead_organic_matter': 10.0
        }

        previous_data = {
            'soil_organic_carbon': 95.0,
            'above_ground_biomass': 45.0,
            'below_ground_biomass': 18.0,
            'dead_organic_matter': 12.0
        }

        changes = self.manager.calculate_stock_changes(current_data, previous_data)

        # Verificar cálculos para cada tipo de estoque
        self.assertEqual(changes['soil_organic_carbon']['change_c'], 5.0)
        self.assertAlmostEqual(
            changes['soil_organic_carbon']['change_co2'],
            5.0 * 44/12,
            places=2
        )

        self.assertEqual(changes['above_ground_biomass']['change_c'], 5.0)
        self.assertEqual(changes['below_ground_biomass']['change_c'], 2.0)
        self.assertEqual(changes['dead_organic_matter']['change_c'], -2.0)

    def test_calculate_amortized_flux(self):
        """
        Testa cálculo de fluxo amortizado.
        """
        stock_changes = {
            'soil_organic_carbon': {
                'change_c': 5.0,
                'change_co2': 18.33,
                'amortization_period': 20
            },
            'above_ground_biomass': {
                'change_c': -10.0,
                'change_co2': -36.67,
                'amortization_period': 20
            }
        }

        amortized_flux = self.manager.calculate_amortized_flux(stock_changes)

        # Verificar amortização linear
        self.assertAlmostEqual(
            amortized_flux['soil_organic_carbon'],
            18.33 / 20,
            places=2
        )
        self.assertAlmostEqual(
            amortized_flux['above_ground_biomass'],
            -36.67 / 20,
            places=2
        )

    def test_is_land_use_change(self):
        """
        Testa detecção de mudança no uso da terra.
        """
        self.assertTrue(
            self.manager.is_land_use_change('forest', 'cropland')
        )
        self.assertTrue(
            self.manager.is_land_use_change('cropland', 'grassland')
        )
        self.assertFalse(
            self.manager.is_land_use_change('cropland', 'cropland')
        )
        self.assertFalse(
            self.manager.is_land_use_change('unknown', 'cropland')
        )