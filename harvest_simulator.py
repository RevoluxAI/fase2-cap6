#!/usr/bin/env python3

import numpy as np
import pandas as pd

class HarvestSimulator:
    """
    Simula a operação de colheita mecanizada de cana-de-açúcar.

    Utiliza modelos estatísticos para gerar dados sintéticos que
    representam perdas em diferentes condições operacionais.
    """

    DEFAULTS = {
        # Parâmetros da máquina
        'speed': 5.5,
        'cutting_height': 0.15,
        'blade_wear': 0.2,

        # Parâmetros ambientais
        'soil_moisture': 20.0,
        'temperature': 25.0,

        # Parâmetros do talhão
        'crop_age': 1,
        'variety': 'RB92579',
        'productivity': 80.0
    }

    # Definição das variações para simulação
    VARIATIONS = {
        'speed': 0.05,
        'cutting_height': 0.08,
        'soil_moisture': 0.1
    }


    def __init__(self, field_params, machine_params, environment_params):
        """
        Inicializa o simulador com parâmetros específicos.

        Args:
            field_params: características do talhão (dict)
            machine_params: configurações da colheitadeira (dict)
            environment_params: condições ambientais (dict)
        """
        self.field_params = field_params or {}
        self.machine_params = machine_params or {}
        self.environment_params = environment_params or {}
        self.results = []


    def get_param(self, name):
        """
        Obtém parâmetro usando ChainMap para definir ordem de busca.

        Args:
            name: Nome do parâmetro a obter

        Returns:
            Valor do parâmetro ou valor padrão
        """
        from collections import ChainMap

        # Define ordem de busca: machine -> environment -> field -> defaults
        params_chain = ChainMap(
            self.machine_params,
            self.environment_params,
            self.field_params,
            self.DEFAULTS
        )

        # busca o parâmetro na cadeia de dicionários
        return params_chain.get(name)

    # Define função para simular variações com ruído normal
    def add_variations(self, param_name, base_value):
        """
        Adiciona variação aleatória a um parâmetro base.

        Args:
            param_name: Nome do parâmetro para obter o fator de variação
            base_value: Valor base a ser variado

        Returns:
            Valor com variação aplicada
        """
        # Verifica se o parâmetro tem variação definida
        # Se não houver variação, retorna o valor base
        # Se houver, aplica a variação usando distribuição normal
        variation = self.VARIATIONS.get(param_name)
        return (
            base_value if variation is None
            else base_value * np.random.normal(1.0, variation)
        )

    def run_simulation(self, num_iterations=100):
        """
        Executa a simulação por um número definido de iterações.

        Args:
            num_iterations: número de pontos de dados a simular

        Returns:
            DataFrame com resultados da simulação
        """
        # Pré-carrega todos os parâmetros de uma vez
        params = {name: self.get_param(name) for name in self.DEFAULTS}

        def generate_points():
            """Gera pontos de simulação eficientemente"""
            for i in range(num_iterations):
                # Cria ponto de simulação com todos os parâmetros
                point = self._create_simulation_point(params, i)

                # Calcula perdas para este ponto
                losses = self._calculate_losses(point)

                # Combina parâmetros e perdas num único dicionário
                yield {**point, **losses}

        # Cria o DataFrame diretamente do generator
        self.results = pd.DataFrame(generate_points())
        return self.results

    def _create_simulation_point(self, base_params, point_id):
        """
        Cria um ponto de simulação com variações e coordenadas.

        Args:
            base_params: Parâmetros base da simulação
            point_id:    Identificador único do ponto

        Returns:
            Dicionário completo do ponto
        """
        # Começa com uma cópia dos parâmetros base
        point = base_params.copy()

        for param_name in self.VARIATIONS:
            # Adiciona variações a cada parâmetro
            # Se o parâmetro não estiver na lista de variações, ignora
            if param_name in point:
                base_value = point[param_name]
                point[param_name] = (
                    self.add_variations(param_name, base_value)
                )

        # Adiciona coordenadas usando sistema de grade
        grid_size = 10 # Tamanho da célula da grade em unidades

        # Adiciona coordenadas do ponto
        point.update({
            'point_id': point_id,
            'x': (point_id %  10 ) * grid_size, # 10 colunas na grade
            'y': (point_id // 10 ) * grid_size  # Linhas necessárias
        })

        return point


    def _calculate_losses(self, params):
        """
        Calcula todas as perdas para um conjunto de parâmetros.

        Args:
            params: Dicionário com parâmetros do ponto

        Returns:
            Dicionário com perdas calculadas
        """
        # Calcula os diferentes tipos de perda (visíveis e invisíveis)
        # e armazena os resultados em um dicionário
        visible_loss = self._calculate_visible_loss(**params)
        invisible_loss = self._calculate_invisible_loss(**params)

        # Calcula perdas totais
        total_loss = visible_loss + invisible_loss
        total_loss_percent = (total_loss / params["productivity"]) * 100

        return {
            'visible_loss':       visible_loss,
            'invisible_loss':     invisible_loss,
            'total_loss':         total_loss,
            'total_loss_percent': total_loss_percent
        }


    def _calculate_factor(
        self, param_value, threshold, impact, inverse=False
    ):
        """
        Calcula o fator de ajuste com base no valor do parâmetro.

        Args:
            param_value: Valor atual do parâmetro
            threshold: Valor limiar para aplicação do impacto. Deve ser
                       maior que zero. Se for zero ou negativo, uma exceção
                       será levantada.
            impact: Magnitude do impacto no fator
            inverse: Se True, relação é inversa (mais parâmetro = menos perda)

        Returns:
            Fator calculado

        Raises:
            ValueError: Se o threshold for zero ou negativo.
            TypeError: Se param_value ou threshold não forem numéricos.
        """
        if not isinstance(param_value, (int, float)) or not isinstance(
            threshold, (int, float)
        ):
            raise TypeError(
                "Both param_value and threshold must be numeric."
            )

        if threshold <= 0:
            raise ValueError(
                "O limiar deve ser maior que zero para evitar "
                "normalização inválida."
            )

        delta = max(0, param_value - threshold)

        # Normaliza o delta em relação ao limiar, já validado como > 0
        normalized_data = delta / threshold

        # Define o sinal do impacto: -1 para relação inversa, +1 para direta
        sign = -1 if inverse else 1

        return 1.0 + sign * (impact * normalized_data)


    def _calculate_loss_base(self, params, base_rate, factor_configs):
        """
        Método base para cálculo de perdas com diferentes configurações

        Args:
            params: Dicionário com parâmetros
            base_rate: Taxa base de perda em condições ideiais
            factor_configs: Lista de configurações para fatores de ajuste

        Returns:
            Valor de perda em ton/ha
        """
        # Inicializa com taxa base
        loss_rate = base_rate

        # Aplica cada fator de ajuste
        for config in factor_configs:
            param_name = config['param']
            threshold  = config['threshold']
            impact     = config['impact']
            inverse    = config.get('inverse', False)

            # Obtém o valor do parâmetro
            value = params[param_name]

            # Calcula o fator do parâmetro
            factor = self._calculate_factor(value, threshold, impact, inverse)

            # Aplica à taxa de perda
            loss_rate *= factor

        # Converte para valor absoluto
        return loss_rate * params['productivity']

    def _calculate_visible_loss(self, **params):
        """
        Calcula perdas visíveis com base nos parâmetros.

        Args:
            **params: Dicionário com todos os parâmetros da simulação.
                - speed: Velocidade da colheitadeira (km/h).
                - cutting_height: Altura de corte (m).
                - blade_wear: Desgaste das lâminas (0-1).
                - soil_moisture: Umidade do solo (%).
                - crop_age: Idade do corte/ciclo do canavial (número 1-3):
                    1: primeira colheita após o plantio inicial (cana-planta).
                    2: colheita após a primeira rebrota (primeira cana-soca).
                    3: colheita após a segunda rebrota (segunda cana-soca).

        Returns:
            Valor de perda visível em ton/ha.
        """
        # Configurações para fatores de perda visível
        factor_configs = [
            # Sobre o speed:
            # - Aumenta a perda com o aumento da velocidade
            # - A perda é máxima quando a velocidade é maior que 5 km/h
            # - A perda é mínima quando a velocidade é menor que 5 km/h
            {
                'param': 'speed',
                'threshold': 5.0,
                'impact': 0.3
            },
            # Sobre o cutting_height:
            # - Aumenta a perda com o aumento da altura de corte
            # - A perda é máxima quando a altura de corte é maior que 0.1 m
            # - A perda é mínima quando a altura de corte é menor que 0.1 m
            # - A perda é proporcional à diferença entre o valor atual e o limiar
            # - A perda é inversa, ou seja, quanto maior a altura de corte,
            #   maior a perda
            {
                'param': 'cutting_height',
                'threshold': 0.1,
                'impact': 0.5
            },
            # Sobre o blade_wear:
            # - Aumenta a perda com o aumento do desgaste da lâmina
            # - A perda é máxima quando o desgaste é maior que 0.2
            # - A perda é mínima quando o desgaste é menor que 0.2
            {
                'param': 'blade_wear',
                'threshold': 0.1,
                'impact': 0.2
            },
            # Sobre o soil_moisture:
            # - Aumenta a perda com a diminuição da umidade do solo
            # - A perda é máxima quando a umidade do solo é menor que 15%
            # - A perda é mínima quando a umidade do solo é maior que 15%
            {
                'param': 'soil_moisture',
                'threshold': 15.0,
                'impact': 0.2,
                'inverse': True
            },
            # Sobre o crop_age:
            # - Aumenta a perda com o aumento da idade do canavial
            # - A perda é máxima quando a idade do canavial é maior que 1 ano
            # - A perda é mínima quando a idade do canavial é menor que 1 ano
            {
                'param': 'crop_age',
                'threshold': 1,
                'impact': 0.1
            }
        ]

        # Usa o método base com taxa base para perdas visíveis
        return self._calculate_loss_base(params,
                                         base_rate=0.02,
                                         factor_configs=factor_configs)

    def _calculate_invisible_loss(self, params):
        """
        Calcula todas as perdas para um conjunto de parâmetros.

        Args:
            params: Dicionário com parâmetros do ponto

        Returns:
            Dicionário com perdas calculadas
        """
        # Configurações para fatores de perda invisível
        # Estes fatores têm pesos diferentes das perdas visíveis
        # e podem ser ajustados conforme necessário
        factor_configs = [
            {
                'param': 'cutting_height',
                'threshold': 0.1,
                'impact': 0.8
            },
            {
                'param': 'speed',
                'threshold': 5.0,
                'impact': 0.1
            },
            {
                'param': 'soil_moisture',
                'threshold': 15.0,
                'impact': 0.2,
                'inverse': True
            }
        ]

        '''
        Utiliza o método base para calcular perdas invisíveis.

        A taxa base de 0.01 (1%) reflete uma estimativa conservadora
        para perdas invisíveis em condições ideais.

        Este valor pode ser ajustado para representar melhor as
        condições específicas do campo e da operação, considerando
        que perdas invisíveis geralmente têm menor impacto na
        produtividade total.
        '''
        return self._calculate_loss_base(**params,
                                         base_rate=0.01,
                                         factor_configs=factor_configs)