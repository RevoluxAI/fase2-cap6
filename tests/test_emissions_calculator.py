#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4. Testes Unitários
# tests/test_emissions_calculator.py
import unittest
from src.ghg_inventory.boundary_manager import BoundaryManager
from src.ghg_inventory.emissions_calculator import EmissionsCalculator

class TestEmissionsCalculator(unittest.TestCase):
    """
    Testes para o calculador de emissões GHG.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        config = {
            'organizational_boundary': 'operational_control',
            'calculation_tier': 'tier1'
        }
        self.boundary_manager = BoundaryManager(config)

        self.emission_factors = {
            'diesel': {
                'CO2': 2.68,
                'CH4': 0.0001,
                'N2O': 0.0001,
                'CO2e': 2.71
            }
        }

        self.calculator = EmissionsCalculator(
            self.boundary_manager,
            self.emission_factors
        )

    def test_calculate_mechanical_emissions(self):
        """
        Testa cálculo de emissões mecânicas.
        """
        fuel_data = {'diesel': 100.0}  # 100 litros

        emissions = self.calculator.calculate_mechanical_emissions(fuel_data)

        self.assertAlmostEqual(emissions['CO2'], 268.0, places=1)
        self.assertAlmostEqual(emissions['CH4'], 0.01, places=3)
        self.assertAlmostEqual(emissions['N2O'], 0.01, places=3)
        self.assertAlmostEqual(emissions['CO2e'], 271.0, places=1)

    def test_calculate_non_mechanical_emissions_soil(self):
        """
        Testa cálculo de emissões não-mecânicas de solos.
        """
        activity_data = {
            'soil_management': {
                'nitrogen_applied': 100.0  # 100 kg N
            }
        }

        emissions = self.calculator.calculate_non_mechanical_emissions(activity_data)

        # Verifica cálculo de N2O usando fator IPCC Tier 1
        self.assertGreater(emissions['N2O'], 0)
        self.assertGreater(emissions['CO2e'], 0)

    def test_calculate_non_mechanical_emissions_burning(self):
        """
        Testa cálculo de emissões não-mecânicas da queima de resíduos.
        """
        activity_data = {
            'residue_burning': {
                'biomass_burned': 1000.0  # 1000 kg
            }
        }

        emissions = self.calculator.calculate_non_mechanical_emissions(activity_data)

        # Verifica cálculo de CH4 e N2O usando fatores IPCC Tier 1
        self.assertGreater(emissions['CH4'], 0)
        self.assertGreater(emissions['N2O'], 0)
        self.assertGreater(emissions['CO2e'], 0)
