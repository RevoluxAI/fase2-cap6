#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Processador de Dados e Motor de Recomendações
class RecommendationEngine:
    """
    Gera recomendações para redução de perdas na colheita.
    """

    def __init__(self, config=None):
        """
        Inicializa o motor de recomendações.

        Args:
            config (dict): Configurações para recomendações
        """
        self.config = config or {}

        # Recomendações por tipo de fator
        self.recommendations_by_factor = {
            'harvester_speed': {
                'above': [
                    "Reduzir a velocidade da colheitadeira para a faixa ideal de "
                    "4,5-6,5 km/h",
                    "Considerar condições do terreno ao ajustar a velocidade",
                    "Monitorar qualidade do corte a cada ajuste de velocidade"
                ],
                'below': [
                    "Aumentar gradualmente a velocidade da colheitadeira até "
                    "atingir a faixa ideal de 4,5-6,5 km/h",
                    "Verificar se há obstáculos ou condições que impedem "
                    "velocidade adequada",
                    "Avaliar se a baixa velocidade está relacionada a problemas "
                    "mecânicos"
                ]
            },
            'cutting_height': {
                'above': [
                    "Reduzir altura de corte para a faixa ideal de 20-30 mm",
                    "Verificar condições dos discos de corte e facas",
                    "Calibrar sensores de altura de corte basal"
                ],
                'below': [
                    "Aumentar altura de corte para evitar contato com o solo",
                    "Verificar desgaste dos discos de corte basal",
                    "Ajustar flutuação do cortador de base conforme densidade "
                    "do canavial"
                ]
            },
            'soil_humidity': {
                'above': [
                    "Aguardar condições mais secas se possível",
                    "Aumentar ligeiramente a altura de corte em solos muito úmidos",
                    "Reduzir velocidade em áreas com excesso de umidade"
                ],
                'below': [
                    "Considerar irrigação pré-colheita se viável",
                    "Ajustar profundidade de corte para evitar poeira excessiva",
                    "Programar colheita para períodos com maior umidade do ar "
                    "(início da manhã/final da tarde)"
                ]
            },
            'temperature': {
                'above': [
                    "Priorizar operações noturnas quando temperatura for excessiva",
                    "Garantir manutenção adequada dos sistemas hidráulicos que "
                    "podem superaquecer",
                    "Monitorar temperatura do motor da colheitadeira"
                ],
                'below': [
                    "Aguardar aumento da temperatura se possível",
                    "Verificar se a baixa temperatura está afetando sistemas "
                    "hidráulicos",
                    "Realizar aquecimento adequado da máquina antes da operação"
                ]
            },
            'wind_speed': {
                'above': [
                    "Reduzir velocidade da colheitadeira em condições de vento forte",
                    "Ajustar direção de colheita considerando direção do vento",
                    "Suspender operação se velocidade do vento for extrema"
                ],
                'below': []  # Vento baixo não gera recomendações específicas
            }
        }

        # Recomendações gerais
        self.general_recommendations = [
            "Realizar manutenção preventiva regular dos equipamentos de colheita",
            "Treinar operadores para identificar e responder a condições variáveis",
            "Implementar sistema de monitoramento contínuo de perdas",
            "Ajustar configurações da colheitadeira de acordo com variedade da cana"
        ]

        # Histórico de recomendações para evitar repetições frequentes
        self.recommendation_history = []

    def generate_recommendations(self, analysis_results):
        """
        Gera recomendações baseadas nos resultados da análise.

        Args:
            analysis_results (dict): Resultados da análise de dados

        Returns:
            dict: Recomendações para redução de perdas
        """
        if 'problematic_factors' not in analysis_results:
            return {
                'error': 'Dados de análise insuficientes ou inválidos'
            }

        loss_category = analysis_results.get('loss_category', 'unknown')
        problematic_factors = analysis_results.get('problematic_factors', [])

        # Recomendações específicas baseadas nos fatores problemáticos
        specific_recommendations = []
        for factor_data in problematic_factors:
            factor = factor_data.get('factor')
            direction = factor_data.get('direction')

            if (factor in self.recommendations_by_factor and
                direction in self.recommendations_by_factor[factor]):
                # Adiciona recomendações para este fator/direção
                factor_recommendations = self.recommendations_by_factor[factor][direction]

                # Se a severidade for alta, prioriza a primeira recomendação
                if factor_data.get('severity', 0) > 0.7:
                    if factor_recommendations:
                        specific_recommendations.append({
                            'factor': factor,
                            'text': factor_recommendations[0],
                            'priority': 'high'
                        })
                # Se média, adiciona até duas recomendações
                elif factor_data.get('severity', 0) > 0.5:
                    for i, rec in enumerate(factor_recommendations[:2]):
                        specific_recommendations.append({
                            'factor': factor,
                            'text': rec,
                            'priority': 'medium' if i == 0 else 'low'
                        })
                # Se baixa, adiciona apenas uma recomendação
                else:
                    if factor_recommendations:
                        specific_recommendations.append({
                            'factor': factor,
                            'text': factor_recommendations[0],
                            'priority': 'low'
                        })

        # Adiciona recomendações gerais se poucas recomendações específicas
        general_count = 0
        if len(specific_recommendations) < 3:
            # Filtra para não repetir recomendações recentes
            filtered_general = [
                rec for rec in self.general_recommendations
                if rec not in [r.get('text') for r in self.recommendation_history[-5:]]
            ]

            # Adiciona recomendações gerais
            for rec in filtered_general[:3 - len(specific_recommendations)]:
                specific_recommendations.append({
                    'factor': 'general',
                    'text': rec,
                    'priority': 'medium'
                })
                general_count += 1

        # Atualiza histórico de recomendações
        self.recommendation_history.extend(specific_recommendations)
        if len(self.recommendation_history) > 20:
            self.recommendation_history = self.recommendation_history[-20:]

        # Monta resposta
        result = {
            'timestamp': analysis_results.get('timestamp'),
            'loss_estimate': analysis_results.get('loss_estimate'),
            'loss_category': loss_category,
            'recommendations': specific_recommendations,
            'summary': {
                'specific_count': len(specific_recommendations) - general_count,
                'general_count': general_count,
                'total_count': len(specific_recommendations)
            }
        }

        return result