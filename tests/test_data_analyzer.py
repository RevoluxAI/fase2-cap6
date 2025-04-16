#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4. Testes Unitários
# tests/test_data_analyzer.py
import unittest
from datetime import datetime
from src.processing.data_analyzer import DataAnalyzer

class TestDataAnalyzer(unittest.TestCase):
    """
    Testes para o analisador de dados.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        self.config = {
            'loss_thresholds': {
                'high': 15.0,
                'medium': 10.0,
                'low': 5.0
            }
        }

        self.analyzer = DataAnalyzer(self.config)
        self.timestamp = datetime.now()

    def test_loss_calculation(self):
        """
        Testa cálculo de estimativa de perda.
        """
        # Caso ideal - perdas mínimas
        ideal_data = {
            'harvester_speed': 5.5,  # dentro da faixa ótima (4.5-6.5)
            'cutting_height': 25,    # dentro da faixa ótima (20-30)
            'soil_humidity': 35,     # dentro da faixa ótima (25-45)
            'temperature': 25,       # dentro da faixa ótima (20-30)
            'wind_speed': 5          # dentro da faixa ótima (0-10)
        }

        result_ideal = self.analyzer.process_sensor_data(self.timestamp, ideal_data)

        # Em condições ideais, deve ter apenas perda base
        self.assertLessEqual(result_ideal['loss_estimate'], 6.0)
        self.assertEqual(result_ideal['loss_category'], 'low')
        self.assertEqual(len(result_ideal['problematic_factors']), 0)

        # Caso crítico - altas perdas
        critical_data = {
            'harvester_speed': 9.0,  # muito acima do ótimo
            'cutting_height': 45,    # muito acima do ótimo
            'soil_humidity': 70,     # muito acima do ótimo
            'temperature': 38,       # muito acima do ótimo
            'wind_speed': 20         # muito acima do ótimo
        }

        result_critical = self.analyzer.process_sensor_data(
            self.timestamp,
            critical_data
        )

        # Em condições críticas, deve ter perdas altas
        self.assertGreaterEqual(result_critical['loss_estimate'], 15.0)
        self.assertEqual(result_critical['loss_category'], 'high')
        self.assertGreater(len(result_critical['problematic_factors']), 0)

    def test_trend_analysis(self):
        """
        Testa análise de tendências.
        """
        # Adiciona dados históricos simulados
        for i in range(10):
            sensor_data = {
                'harvester_speed': 5.0 + i * 0.2,  # Aumenta gradualmente
                'cutting_height': 25,
                'soil_humidity': 35,
                'temperature': 25,
                'wind_speed': 5
            }

            self.analyzer.process_sensor_data(
                datetime.now(),
                sensor_data
            )

        # Análise de tendência
        trend_results = self.analyzer.analyze_trends()

        # Deve identificar tendência de aumento nas perdas
        self.assertIsNotNone(trend_results)
        self.assertTrue('loss_statistics' in trend_results)
        self.assertTrue('trend' in trend_results['loss_statistics'])