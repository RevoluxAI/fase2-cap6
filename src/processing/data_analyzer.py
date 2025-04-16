#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 2. Processador de Dados e Motor de Recomendações
# src/processing/data_analyzer.py
import numpy as np
from datetime import datetime, timedelta

class DataAnalyzer:
    """
    Analisa dados dos sensores e identifica padrões relacionados a perdas.
    """

    def __init__(self, config=None):
        """
        Inicializa o analisador de dados.

        Args:
            config (dict): Configurações para análise
        """
        self.config = config or {}
        self.loss_thresholds = self.config.get('loss_thresholds', {
            'high': 15.0,
            'medium': 10.0,
            'low': 5.0
        })

        # Fatores que influenciam perda na colheita de cana
        self.loss_factors = {
            'harvester_speed': {
                'weight': 0.3,
                'optimal_range': (4.5, 6.5),  # km/h
                'critical_above': 7.0,        # km/h
                'critical_below': 3.5         # km/h
            },
            'cutting_height': {
                'weight': 0.25,
                'optimal_range': (20, 30),    # mm
                'critical_above': 40,         # mm
                'critical_below': 15          # mm
            },
            'soil_humidity': {
                'weight': 0.20,
                'optimal_range': (25, 45),    # %
                'critical_above': 60,         # %
                'critical_below': 15          # %
            },
            'temperature': {
                'weight': 0.15,
                'optimal_range': (20, 30),    # °C
                'critical_above': 35,         # °C
                'critical_below': 15          # °C
            },
            'wind_speed': {
                'weight': 0.10,
                'optimal_range': (0, 10),     # km/h
                'critical_above': 15,         # km/h
                'critical_below': None        # Vento baixo não é crítico
            }
        }

        # Histórico de dados
        self.data_history = {
            'timestamps': [],
            'sensor_readings': {},
            'calculated_losses': []
        }

    def process_sensor_data(self, timestamp, sensor_data):
        """
        Processa dados dos sensores e estima perdas.

        Args:
            timestamp (datetime): Momento da leitura
            sensor_data (dict): Leituras dos sensores

        Returns:
            dict: Resultados da análise
        """
        # Armazena dados no histórico
        self.data_history['timestamps'].append(timestamp)

        # Inicializa entrada para este timestamp se necessário
        for sensor, reading in sensor_data.items():
            if sensor not in self.data_history['sensor_readings']:
                self.data_history['sensor_readings'][sensor] = []

            # Armazena valor numérico do sensor
            if isinstance(reading, dict) and 'value' in reading:
                self.data_history['sensor_readings'][sensor].append(reading['value'])
            else:
                self.data_history['sensor_readings'][sensor].append(reading)

        # Calcula estimativa de perda
        loss_estimate = self._calculate_loss_estimate(sensor_data)
        self.data_history['calculated_losses'].append(loss_estimate)

        # Identifica fatores problemáticos
        problematic_factors = self._identify_problematic_factors(sensor_data)

        return {
            'timestamp': timestamp.isoformat(),
            'loss_estimate': loss_estimate,
            'loss_category': self._categorize_loss(loss_estimate),
            'problematic_factors': problematic_factors
        }

    def _calculate_loss_estimate(self, sensor_data):
        """
        Calcula estimativa de perda baseada nos dados dos sensores.

        Args:
            sensor_data (dict): Leituras dos sensores

        Returns:
            float: Estimativa de perda em percentual
        """
        base_loss = 5.0  # Perda base mesmo em condições ideais
        additional_loss = 0.0

        for factor, properties in self.loss_factors.items():
            # Verifica se há dados para este fator
            if factor not in sensor_data:
                continue

            value = sensor_data[factor]
            if isinstance(value, dict) and 'value' in value:
                value = value['value']

            # Calcula desvio do valor ótimo
            min_optimal, max_optimal = properties['optimal_range']

            if value < min_optimal:
                # Desvio abaixo do ótimo
                if properties['critical_below'] is not None:
                    deviation = (min_optimal - value) / (min_optimal -
                                                        properties['critical_below'])
                    deviation = max(0, min(1, deviation))
                    additional_loss += properties['weight'] * 10 * deviation
            elif value > max_optimal:
                # Desvio acima do ótimo
                if properties['critical_above'] is not None:
                    deviation = (value - max_optimal) / (properties['critical_above'] -
                                                       max_optimal)
                    deviation = max(0, min(1, deviation))
                    additional_loss += properties['weight'] * 10 * deviation

        # Considerar fatores adicionais que podem reduzir ou aumentar perdas
        if 'is_raining' in sensor_data and sensor_data['is_raining']:
            additional_loss += 2.0  # Chuva aumenta perdas

        total_loss = base_loss + additional_loss
        return min(total_loss, 25.0)  # Limita a 25% de perda máxima

    def _categorize_loss(self, loss_value):
        """
        Categoriza a perda estimada.

        Args:
            loss_value (float): Valor da perda estimada

        Returns:
            str: Categoria de perda
        """
        if loss_value >= self.loss_thresholds['high']:
            return 'high'
        elif loss_value >= self.loss_thresholds['medium']:
            return 'medium'
        elif loss_value >= self.loss_thresholds['low']:
            return 'low'
        else:
            return 'minimal'

    def _identify_problematic_factors(self, sensor_data):
        """
        Identifica fatores que mais contribuem para as perdas.

        Args:
            sensor_data (dict): Leituras dos sensores

        Returns:
            list: Fatores problemáticos ordenados por severidade
        """
        problematic_factors = []

        for factor, properties in self.loss_factors.items():
            if factor not in sensor_data:
                continue

            value = sensor_data[factor]
            if isinstance(value, dict) and 'value' in value:
                value = value['value']

            min_optimal, max_optimal = properties['optimal_range']

            # Verifica se está fora da faixa ótima
            if value < min_optimal and properties['critical_below'] is not None:
                severity = (min_optimal - value) / (min_optimal -
                                                  properties['critical_below'])
                severity = max(0, min(1, severity))

                if severity > 0.3:  # Significativo o suficiente para reportar
                    problematic_factors.append({
                        'factor': factor,
                        'value': value,
                        'optimal_range': properties['optimal_range'],
                        'severity': severity,
                        'direction': 'below'
                    })
            elif value > max_optimal and properties['critical_above'] is not None:
                severity = (value - max_optimal) / (properties['critical_above'] -
                                                  max_optimal)
                severity = max(0, min(1, severity))

                if severity > 0.3:  # Significativo o suficiente para reportar
                    problematic_factors.append({
                        'factor': factor,
                        'value': value,
                        'optimal_range': properties['optimal_range'],
                        'severity': severity,
                        'direction': 'above'
                    })

        # Ordena por severidade (mais severo primeiro)
        problematic_factors.sort(key=lambda x: x['severity'], reverse=True)

        return problematic_factors

    def analyze_trends(self, time_window=None):
        """
        Analisa tendências nas perdas e fatores contribuintes.

        Args:
            time_window (int): Janela de tempo em horas para análise

        Returns:
            dict: Resultados da análise de tendências
        """
        if not self.data_history['timestamps']:
            return {'error': 'Dados insuficientes para análise de tendências'}

        # Define janela de tempo para análise
        if time_window is None:
            # Usa todos os dados disponíveis
            data_indices = range(len(self.data_history['timestamps']))
        else:
            # Filtra por janela de tempo
            current_time = self.data_history['timestamps'][-1]
            cutoff_time = current_time - timedelta(hours=time_window)

            data_indices = [i for i, ts in enumerate(self.data_history['timestamps'])
                           if ts >= cutoff_time]

        if not data_indices:
            return {'error': 'Dados insuficientes para o período especificado'}

        # Calcula estatísticas de perda
        losses = [self.data_history['calculated_losses'][i] for i in data_indices]
        avg_loss = np.mean(losses)
        min_loss = np.min(losses)
        max_loss = np.max(losses)

        # Analisa tendência de perda
        if len(losses) >= 3:
            # Cálculo de tendência simples (comparando primeira e segunda metade)
            mid_point = len(losses) // 2
            first_half_avg = np.mean(losses[:mid_point])
            second_half_avg = np.mean(losses[mid_point:])

            if second_half_avg > first_half_avg * 1.1:
                loss_trend = 'increasing'
            elif second_half_avg < first_half_avg * 0.9:
                loss_trend = 'decreasing'
            else:
                loss_trend = 'stable'
        else:
            loss_trend = 'insufficient_data'

        # Identifica fatores frequentemente problemáticos
        factor_frequency = {}
        for i in data_indices:
            factors = self._identify_problematic_factors({
                factor: self.data_history['sensor_readings'][factor][i]
                for factor in self.data_history['sensor_readings']
                if i < len(self.data_history['sensor_readings'][factor])
            })

            for factor_data in factors:
                factor_name = factor_data['factor']
                if factor_name not in factor_frequency:
                    factor_frequency[factor_name] = 0
                factor_frequency[factor_name] += 1

        # Ordena fatores por frequência
        common_factors = sorted(
            factor_frequency.items(),
            key=lambda x: x[1],
            reverse=True
        )

        return {
            'time_period': {
                'start': self.data_history['timestamps'][data_indices[0]].isoformat(),
                'end': self.data_history['timestamps'][data_indices[-1]].isoformat(),
                'samples': len(data_indices)
            },
            'loss_statistics': {
                'average': avg_loss,
                'minimum': min_loss,
                'maximum': max_loss,
                'trend': loss_trend
            },
            'common_problematic_factors': [
                {'factor': f[0], 'frequency': f[1]}
                for f in common_factors if f[1] > 0
            ]
        }