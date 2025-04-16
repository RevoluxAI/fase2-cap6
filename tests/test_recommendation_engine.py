#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4. Testes Unitários
# tests/test_recommendation_engine.py
import unittest
from src.processing.recommendation_engine import RecommendationEngine

class TestRecommendationEngine(unittest.TestCase):
    """
    Testes para o motor de recomendações.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        self.engine = RecommendationEngine()

    def test_generate_recommendations(self):
        """
        Testa geração de recomendações baseadas em análise.
        """
        # Análise com fatores problemáticos
        analysis_results = {
            'timestamp': '2025-04-15T14:30:45',
            'loss_estimate': 15.5,
            'loss_category': 'high',
            'problematic_factors': [
                {
                    'factor': 'harvester_speed',
                    'value': 8.5,
                    'optimal_range': (4.5, 6.5),
                    'severity': 0.8,
                    'direction': 'above'
                },
                {
                    'factor': 'soil_humidity',
                    'value': 65.0,
                    'optimal_range': (25, 45),
                    'severity': 0.7,
                    'direction': 'above'
                }
            ]
        }

        recommendations = self.engine.generate_recommendations(analysis_results)

        # Verifica se recomendações foram geradas
        self.assertIsNotNone(recommendations)
        self.assertTrue('recommendations' in recommendations)
        self.assertGreater(len(recommendations['recommendations']), 0)

        # Verifica se recomendações correspondem aos fatores problemáticos
        harvester_recommendations = [
            rec for rec in recommendations['recommendations']
            if rec['factor'] == 'harvester_speed'
        ]

        self.assertGreater(len(harvester_recommendations), 0)

        # Verifica se recomendação de alta severidade tem prioridade alta
        high_priority_recs = [
            rec for rec in recommendations['recommendations']
            if rec['priority'] == 'high'
        ]

        self.assertGreaterEqual(len(high_priority_recs), 1)

    def test_empty_analysis(self):
        """
        Testa comportamento com análise vazia.
        """
        empty_analysis = {}

        recommendations = self.engine.generate_recommendations(empty_analysis)

        # Deve retornar erro para análise vazia
        self.assertTrue('error' in recommendations)

    def test_minimal_losses(self):
        """
        Testa recomendações com perdas mínimas.
        """
        minimal_analysis = {
            'timestamp': '2025-04-15T14:30:45',
            'loss_estimate': 3.5,
            'loss_category': 'minimal',
            'problematic_factors': []
        }

        recommendations = self.engine.generate_recommendations(minimal_analysis)

        # Deve ter apenas recomendações gerais
        self.assertTrue('recommendations' in recommendations)
        self.assertGreaterEqual(len(recommendations['recommendations']), 1)

        # Todas recomendações devem ser gerais
        for rec in recommendations['recommendations']:
            self.assertEqual(rec['factor'], 'general')