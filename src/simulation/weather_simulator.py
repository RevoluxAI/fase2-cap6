#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import math

class WeatherSimulator:
    """
    Simula condições climáticas que afetam a colheita de cana-de-açúcar.
    """

    def __init__(self, config):
        """
        Inicializa o simulador de condições climáticas.

        Args:
            config (dict): Configurações do simulador
        """
        self.config = config
        self.region = config.get('region', 'southeast_brazil')
        self.season = config.get('season', 'harvest')
        self.temperature_range = config.get('temperature_range', (15, 32))
        self.humidity_range = config.get('humidity_range', (40, 95))
        self.wind_speed_range = config.get('wind_speed_range', (0, 25))
        self.precipitation_probability = config.get('precipitation_probability', 0.2)
        self.precipitation_range = config.get('precipitation_range', (0, 30))

        # Parâmetros para simulação contínua
        self.current_temperature = None
        self.current_humidity = None
        self.current_wind_speed = None
        self.current_precipitation = 0
        self.is_raining = False

        # Inicializa condições
        self._initialize_conditions()

    def _initialize_conditions(self):
        """
        Inicializa condições climáticas base.
        """
        # Temperatura inicial dentro da faixa definida
        self.current_temperature = random.uniform(
            self.temperature_range[0],
            self.temperature_range[1]
        )

        # Umidade inicial dentro da faixa definida
        self.current_humidity = random.uniform(
            self.humidity_range[0],
            self.humidity_range[1]
        )

        # Velocidade do vento inicial dentro da faixa definida
        self.current_wind_speed = random.uniform(
            self.wind_speed_range[0],
            self.wind_speed_range[1]
        )

    def get_weather_conditions(self, timestamp):
        """
        Retorna condições climáticas simuladas para o momento específico.

        Args:
            timestamp (datetime): Momento para simular as condições

        Returns:
            dict: Condições climáticas simuladas
        """
        # Atualiza condições climáticas de forma realista
        self._update_conditions(timestamp)

        return {
            'timestamp': timestamp.isoformat(),
            'temperature': round(self.current_temperature, 1),
            'humidity': round(self.current_humidity, 1),
            'wind_speed': round(self.current_wind_speed, 1),
            'precipitation': round(self.current_precipitation, 1),
            'is_raining': self.is_raining
        }

    def _update_conditions(self, timestamp):
        """
        Atualiza condições climáticas de forma realista.

        Args:
            timestamp (datetime): Momento atual
        """
        # Variação diária de temperatura (mais quente durante o dia)
        hour = timestamp.hour + timestamp.minute / 60
        daily_temp_factor = math.sin((hour - 6) * math.pi / 12) * 0.5 + 0.5
        target_temp = (self.temperature_range[0] +
                      (self.temperature_range[1] - self.temperature_range[0]) *
                      daily_temp_factor)

        # Ajusta temperatura gradualmente
        self.current_temperature += (target_temp - self.current_temperature) * 0.3

        # Adiciona variação aleatória
        self.current_temperature += random.uniform(-0.5, 0.5)

        # Limita temperatura à faixa configurada
        self.current_temperature = max(
            self.temperature_range[0],
            min(self.temperature_range[1], self.current_temperature)
        )

        # Chance de precipitação baseada na umidade atual
        rain_chance = self.precipitation_probability
        if self.current_humidity > 80:
            rain_chance *= 2

        # Define se está chovendo
        if random.random() < rain_chance:
            if not self.is_raining:
                self.is_raining = True
            self.current_precipitation = random.uniform(
                self.precipitation_range[0],
                self.precipitation_range[1]
            )
            # Aumenta umidade durante chuva
            self.current_humidity = min(
                self.current_humidity + 10,
                self.humidity_range[1]
            )
        else:
            self.is_raining = False
            self.current_precipitation = 0

            # Umidade tende a seguir padrão inverso da temperatura
            target_humidity = self.humidity_range[1] - (
                (self.humidity_range[1] - self.humidity_range[0]) * daily_temp_factor
            )
            self.current_humidity += (target_humidity - self.current_humidity) * 0.2

        # Adiciona variação aleatória na umidade
        self.current_humidity += random.uniform(-2, 2)

        # Limita umidade à faixa configurada
        self.current_humidity = max(
            self.humidity_range[0],
            min(self.humidity_range[1], self.current_humidity)
        )

        # Atualiza velocidade do vento
        # Tende a ser mais forte durante o dia
        wind_factor = 0.5 + daily_temp_factor * 0.5
        target_wind = (self.wind_speed_range[0] +
                      (self.wind_speed_range[1] - self.wind_speed_range[0]) *
                      wind_factor)

        # Ajusta velocidade gradualmente
        self.current_wind_speed += (target_wind - self.current_wind_speed) * 0.1

        # Adiciona variação aleatória
        self.current_wind_speed += random.uniform(-1, 1)

        # Limita velocidade à faixa configurada
        self.current_wind_speed = max(
            self.wind_speed_range[0],
            min(self.wind_speed_range[1], self.current_wind_speed)
        )

    def get_seasonal_factors(self):
        """
        Retorna fatores específicos da estação que afetam a colheita.

        Returns:
            dict: Fatores sazonais
        """
        if self.season == 'harvest':
            return {
                'soil_dryness': random.uniform(0.6, 0.9),
                'crop_maturity': random.uniform(0.8, 1.0),
                'pest_presence': random.uniform(0.1, 0.3)
            }
        elif self.season == 'growth':
            return {
                'soil_dryness': random.uniform(0.3, 0.6),
                'crop_maturity': random.uniform(0.3, 0.7),
                'pest_presence': random.uniform(0.2, 0.5)
            }
        else:
            return {
                'soil_dryness': random.uniform(0.4, 0.7),
                'crop_maturity': random.uniform(0.5, 0.8),
                'pest_presence': random.uniform(0.1, 0.4)
            }