#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Processador de Dados e Motor de Recomendações
import numpy as np
from datetime import timedelta

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



    def analyze_trends(self, analysis_data):
        """
        Analisa tendências nos dados coletados ao longo do tempo.

        Identifica padrões temporais, direção de tendência e fatores
        problemáticos recorrentes.

        Args:
            analysis_data (list): Lista de registros de análise

        Returns:
            dict: Resultados da análise de tendências
        """
        if not analysis_data or len(analysis_data) < 3:
            return {"error": "Dados insuficientes para análise de tendências"}

        # Extrai valores de perda e timestamps
        loss_values = []
        timestamps = []

        for data in analysis_data:
            if 'analysis' in data and 'loss_estimate' in data['analysis']:
                loss_values.append(data['analysis']['loss_estimate'])
                timestamps.append(data['timestamp'])

        if not loss_values:
            return {"error": "Nenhum valor de perda encontrado nos dados"}

        # Análise de tendência
        trend_direction = "stable"
        if len(loss_values) >= 3:
            half = len(loss_values) // 2
            first_half = loss_values[:half]
            second_half = loss_values[half:]

            first_avg = sum(first_half) / len(first_half)
            second_avg = sum(second_half) / len(second_half)

            if second_avg > first_avg * 1.1:
                trend_direction = "increasing"
            elif second_avg < first_avg * 0.9:
                trend_direction = "decreasing"

        # Calcular previsão simples (tendência linear)
        prediction = None
        if len(loss_values) >= 6:
            first_half = loss_values[:len(loss_values)//2]
            last_half = loss_values[len(loss_values)//2:]

            first_avg = sum(first_half) / len(first_half)
            last_avg = sum(last_half) / len(last_half)

            rate = (last_avg - first_avg) / (len(loss_values)//2)

            if abs(rate) > 0.01:  # Mudança significativa
                prediction = last_avg + rate * 5  # Previsão para 5 ciclos à frente
                prediction = max(0, min(25, prediction))  # Limita entre 0 e 25%

        # Análise de fatores problemáticos recorrentes
        common_factors = []
        factor_counts = {}

        for data in analysis_data:
            if ('analysis' in data and 'problematic_factors' in data['analysis']):
                for factor in data['analysis']['problematic_factors']:
                    factor_name = factor.get('factor')
                    if not factor_name:
                        continue

                    if factor_name not in factor_counts:
                        factor_counts[factor_name] = {
                            'count': 0,
                            'severities': [],
                            'directions': []
                        }

                    factor_counts[factor_name]['count'] += 1
                    factor_counts[factor_name]['severities'].append(
                        factor.get('severity', 0))
                    factor_counts[factor_name]['directions'].append(
                        factor.get('direction', ''))

        # Calcula frequência e direção predominante
        for factor_name, data in factor_counts.items():
            frequency = data['count'] / len(analysis_data)
            avg_severity = sum(data['severities']) / len(data['severities'])

            # Direção predominante
            above_count = sum(1 for d in data['directions'] if d == 'above')
            below_count = sum(1 for d in data['directions'] if d == 'below')

            if above_count > below_count:
                direction = "above"
            elif below_count > above_count:
                direction = "below"
            else:
                direction = "variable"

            common_factors.append({
                'factor': factor_name,
                'frequency': frequency,
                'severity': avg_severity,
                'direction': direction
            })

        # Ordena por frequência
        common_factors.sort(key=lambda x: x['frequency'], reverse=True)

        return {
            'loss_values': loss_values,           # Lista de valores de perda
            'timestamps': timestamps,             # Lista de timestamps
            'trend_direction': trend_direction,   # increasing, decreasing, stable
            'prediction': prediction,             # Previsão para próximos ciclos
            'common_factors': common_factors[:3]  # Top 3 fatores mais comuns
        }


    def analyze_harvest_losses(self, loss_data):
        """
        Analisa estatísticas detalhadas de perdas na colheita.

        Args:
            loss_data (list): Lista de registros com dados de perdas

        Returns:
            dict: Resultados da análise de perdas
        """
        if not loss_data:
            return {"error": "Dados insuficientes para análise"}

        # Extrai valores de perda
        loss_values = [data['analysis']['loss_estimate']
                     for data in loss_data
                     if 'analysis' in data and 'loss_estimate' in data['analysis']]

        if not loss_values:
            return {"error": "Nenhum valor de perda encontrado nos dados"}

        # Calcula estatísticas
        avg_loss = sum(loss_values) / len(loss_values)
        min_loss = min(loss_values)
        max_loss = max(loss_values)

        # Determina categoria média
        loss_category = self._categorize_loss(avg_loss)

        # Análise de tendência
        trend = "estável"
        if len(loss_values) >= 3:
            half = len(loss_values) // 2
            first_half = loss_values[:half]
            second_half = loss_values[half:]

            first_avg = sum(first_half) / len(first_half)
            second_avg = sum(second_half) / len(second_half)

            if second_avg > first_avg * 1.1:
                trend = "aumentando"
            elif second_avg < first_avg * 0.9:
                trend = "diminuindo"

        # Distribuição por categorias
        categories = {
            'high': sum(1 for x in loss_values if x >= 15),
            'medium': sum(1 for x in loss_values if 10 <= x < 15),
            'low': sum(1 for x in loss_values if 5 <= x < 10),
            'minimal': sum(1 for x in loss_values if x < 5)
        }

        # Percentuais de cada categoria
        total = len(loss_values)
        category_percentages = {
            cat: (count / total * 100) for cat, count in categories.items()
        }

        # Impacto econômico (cálculo simplificado)
        avg_productivity = 80  # toneladas por hectare
        avg_price = 100        # reais por tonelada
        area = 1               # hectare como referência

        potential_yield = avg_productivity * area
        actual_yield = potential_yield * (1 - avg_loss/100)
        loss_value = (potential_yield - actual_yield) * avg_price

        return {
            "count": len(loss_values),
            "avg_loss": avg_loss,
            "min_loss": min_loss,
            "max_loss": max_loss,
            "loss_category": loss_category,
            "trend": trend,
            "categories": categories,
            "category_percentages": category_percentages,
            "economic_impact": {
                "potential_yield": potential_yield,
                "actual_yield": actual_yield,
                "loss_value": loss_value,
                "unit": "R$/ha"
            }
        }

    def analyze_emissions(self, emission_data):
        """
        Analisa dados de emissões de gases de efeito estufa.

        Args:
            emission_data (dict): Dicionário com séries de emissões

        Returns:
            dict: Resultados da análise de emissões
        """
        results = {}

        # Verifica se há dados para CH₄
        if 'ch4_emission' in emission_data and emission_data['ch4_emission']:
            ch4_values = emission_data['ch4_emission']
            ch4_avg = sum(ch4_values) / len(ch4_values)
            ch4_min = min(ch4_values)
            ch4_max = max(ch4_values)

            # Determina status
            if ch4_avg > 10:
                status = "alta"
            elif ch4_avg > 5:
                status = "média"
            else:
                status = "aceitável"

            # Calcula CO₂ equivalente
            ch4_gwp = 28  # Potencial de aquecimento global de CH₄
            ch4_co2e = ch4_avg * ch4_gwp

            results['ch4'] = {
                "avg": ch4_avg,
                "min": ch4_min,
                "max": ch4_max,
                "status": status,
                "co2e": ch4_co2e
            }

        # Verifica se há dados para NH₃
        if 'nh3_emission' in emission_data and emission_data['nh3_emission']:
            nh3_values = emission_data['nh3_emission']
            nh3_avg = sum(nh3_values) / len(nh3_values)
            nh3_min = min(nh3_values)
            nh3_max = max(nh3_values)

            # Determina status
            if nh3_avg > 8:
                status = "alta"
            elif nh3_avg > 4:
                status = "média"
            else:
                status = "aceitável"

            results['nh3'] = {
                "avg": nh3_avg,
                "min": nh3_min,
                "max": nh3_max,
                "status": status
            }

        return results


    def analyze_factors(self, analysis_data):
        """
        Analisa fatores problemáticos que contribuem para perdas.

        Identifica e classifica os fatores que mais impactam as perdas na
        colheita, calculando frequência, severidade e direção predominante.

        Args:
            analysis_data (list): Lista de registros de análise

        Returns:
            dict: Análise de fatores problemáticos
        """
        if not analysis_data:
            return {"error": "Dados insuficientes para análise"}

        # Extrai fatores problemáticos
        all_factors = {}
        for data in analysis_data:
            if 'analysis' in data and 'problematic_factors' in data['analysis']:
                for factor in data['analysis']['problematic_factors']:
                    factor_name = factor.get('factor')
                    if not factor_name:
                        continue

                    if factor_name not in all_factors:
                        all_factors[factor_name] = {
                            'count': 0,
                            'values': [],
                            'severities': [],
                            'directions': [],
                            'optimal_ranges': []
                        }

                    all_factors[factor_name]['count'] += 1
                    all_factors[factor_name]['values'].append(factor.get('value', 0))
                    all_factors[factor_name]['severities'].append(factor.get('severity', 0))
                    all_factors[factor_name]['directions'].append(factor.get('direction', ''))

                    if 'optimal_range' in factor and factor['optimal_range']:
                        all_factors[factor_name]['optimal_ranges'].append(factor['optimal_range'])

        # Calcula estatísticas para cada fator
        factor_stats = {}
        for factor_name, factor_data in all_factors.items():
            # Evita divisão por zero
            if factor_data['severities']:
                avg_severity = sum(factor_data['severities']) / len(factor_data['severities'])
            else:
                avg_severity = 0

            if factor_data['values']:
                avg_value = sum(factor_data['values']) / len(factor_data['values'])
            else:
                avg_value = 0

            # Determina direção predominante
            above_count = sum(1 for d in factor_data['directions'] if d == 'above')
            below_count = sum(1 for d in factor_data['directions'] if d == 'below')

            if above_count > below_count:
                direction = "above"
            elif below_count > above_count:
                direction = "below"
            else:
                direction = "variable"

            # Determina faixa ótima mais comum
            optimal_range = None
            if factor_data['optimal_ranges']:
                ranges_count = {}
                for r in factor_data['optimal_ranges']:
                    r_tuple = tuple(r)  # Converte lista para tupla (hashable)
                    if r_tuple not in ranges_count:
                        ranges_count[r_tuple] = 0
                    ranges_count[r_tuple] += 1

                if ranges_count:
                    optimal_range = max(ranges_count.items(), key=lambda x: x[1])[0]

            # Calcula frequência
            frequency = factor_data['count'] / len(analysis_data)

            # Calcula impacto estimado
            impact = frequency * avg_severity

            factor_stats[factor_name] = {
                "count": factor_data['count'],
                "frequency": frequency,
                "avg_value": avg_value,
                "avg_severity": avg_severity,
                "severity": avg_severity,  # Adicionado para compatibilidade com a UI
                "direction": direction,
                "optimal_range": optimal_range,
                "impact": impact
            }

        # Ordena fatores por impacto
        sorted_factors = sorted(
            factor_stats.items(),
            key=lambda x: x[1]['impact'],
            reverse=True
        )

        return {
            "total_analyses": len(analysis_data),
            "factors_count": len(all_factors),
            "factors": factor_stats,
            "sorted_factors": [f[0] for f in sorted_factors]
        }


    def generate_consolidated_report(self,
                                   loss_analysis,
                                   emission_analysis,
                                   factor_analysis,
                                   recommendations):
        """
        Gera relatório consolidado com análises e recomendações.

        Args:
            loss_analysis (dict): Resultados da análise de perdas
            emission_analysis (dict): Resultados da análise de emissões
            factor_analysis (dict): Resultados da análise de fatores
            recommendations (list): Lista de recomendações

        Returns:
            dict: Relatório consolidado
        """
        # Determina mensagem de conclusão com base na perda média
        conclusion = ""
        if 'avg_loss' in loss_analysis:
            avg_loss = loss_analysis['avg_loss']
            if avg_loss >= 15:
                conclusion = ("As perdas na colheita estão CRÍTICAS e "
                            "requerem ação URGENTE.")
            elif avg_loss >= 10:
                conclusion = ("As perdas na colheita estão ELEVADAS. "
                            "Recomenda-se implementar ajustes.")
            elif avg_loss >= 5:
                conclusion = ("As perdas na colheita estão em nível MODERADO. "
                            "Atenção aos fatores críticos.")
            else:
                conclusion = ("As perdas na colheita estão em nível ACEITÁVEL. "
                            "Manter monitoramento contínuo.")

        # Seleciona recomendações prioritárias
        priority_recs = []
        if recommendations:
            # Filtra por prioridade
            high_priority = [r for r in recommendations
                           if r.get('priority') == 'high']
            medium_priority = [r for r in recommendations
                             if r.get('priority') == 'medium']

            # Combina e remove duplicados
            seen_texts = set()
            for rec in high_priority + medium_priority:
                text = rec.get('text', '')
                if text and text not in seen_texts:
                    seen_texts.add(text)
                    priority_recs.append(rec)

        # Formata top fatores problemáticos
        top_factors = []
        if 'factors' in factor_analysis and 'sorted_factors' in factor_analysis:
            for factor_name in factor_analysis['sorted_factors'][:3]:
                factor = factor_analysis['factors'][factor_name]
                top_factors.append({
                    "name": factor_name,
                    "frequency": factor['frequency'],
                    "severity": factor['avg_severity'],
                    "direction": factor['direction'],
                    "impact": factor['impact']
                })

        return {
            "loss_summary": loss_analysis,
            "emission_summary": emission_analysis,
            "top_factors": top_factors,
            "priority_recommendations": priority_recs[:5],
            "conclusion": conclusion,
            "next_steps": [
                "Implementar as recomendações de alta prioridade",
                "Ajustar parâmetros operacionais conforme análise de fatores",
                "Realizar nova simulação para verificar eficácia das mudanças",
                "Documentar resultados para comparação futura"
            ]
        }