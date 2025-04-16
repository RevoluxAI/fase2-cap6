#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.3.5. Simulador de Sensores
# src/simulation/sensor_simulator.py
import random
import math
from datetime import datetime, timedelta

class SensorSimulator:
    """
    Implementa simulador de sensores para colheita de cana-de-açúcar.
    """

    def __init__(self, config):
        """
        Inicializa o simulador com configurações específicas.

        Args:
            config (dict): Configurações do simulador
        """
        self.config = config
        self.sensors = {}
        self._setup_sensors()

    def _setup_sensors(self):
        """
        Configura os sensores baseado nas configurações.
        """
        sensors_config = self.config.get('sensors', {})

        # Configuração de sensores operacionais
        if 'operational' in sensors_config:
            for sensor_name, sensor_config in sensors_config['operational'].items():
                self.sensors[sensor_name] = OperationalSensor(
                    name=sensor_name,
                    min_value=sensor_config.get('min_value', 0),
                    max_value=sensor_config.get('max_value', 100),
                    unit=sensor_config.get('unit', ''),
                    noise_factor=sensor_config.get('noise_factor', 0.05)
                )

        # Configuração de sensores ambientais
        if 'environmental' in sensors_config:
            for sensor_name, sensor_config in sensors_config['environmental'].items():
                self.sensors[sensor_name] = EnvironmentalSensor(
                    name=sensor_name,
                    min_value=sensor_config.get('min_value', 0),
                    max_value=sensor_config.get('max_value', 100),
                    unit=sensor_config.get('unit', ''),
                    daily_pattern=sensor_config.get('daily_pattern', False)
                )

        # Configuração de sensores de emissão de GHG
        if 'emissions' in sensors_config:
            for sensor_name, sensor_config in sensors_config['emissions'].items():
                self.sensors[sensor_name] = EmissionsSensor(
                    name=sensor_name,
                    base_value=sensor_config.get('base_value', 0),
                    unit=sensor_config.get('unit', ''),
                    sensitivity=sensor_config.get('sensitivity', {})
                )

    def generate_readings(self, timestamp):
        """
        Gera leituras para o momento especificado.

        Args:
            timestamp (datetime): Momento da leitura

        Returns:
            dict: Leituras de todos os sensores
        """
        readings = {}

        # Obter leituras de sensores operacionais e ambientais
        operational_readings = {}
        environmental_readings = {}

        for sensor_name, sensor in self.sensors.items():
            if isinstance(sensor, OperationalSensor):
                operational_readings[sensor_name] = sensor.read(timestamp)
                readings[sensor_name] = operational_readings[sensor_name]
            elif isinstance(sensor, EnvironmentalSensor):
                environmental_readings[sensor_name] = sensor.read(timestamp)
                readings[sensor_name] = environmental_readings[sensor_name]

        # Gerar leituras de sensores de emissão usando dados operacionais e ambientais
        for sensor_name, sensor in self.sensors.items():
            if isinstance(sensor, EmissionsSensor):
                readings[sensor_name] = sensor.read(
                    timestamp,
                    operational_readings,
                    environmental_readings
                )

        return readings


class OperationalSensor:
    """
    Simula sensores operacionais (velocidade, altura de corte, etc.).
    """

    def __init__(self, name, min_value, max_value, unit, noise_factor=0.05):
        """
        Inicializa sensor operacional.

        Args:
            name (str): Nome do sensor
            min_value (float): Valor mínimo possível
            max_value (float): Valor máximo possível
            unit (str): Unidade de medida
            noise_factor (float): Fator de ruído na leitura
        """
        self.name = name
        self.min_value = min_value
        self.max_value = max_value
        self.unit = unit
        self.noise_factor = noise_factor
        self.current_value = (min_value + max_value) / 2

    def read(self, timestamp):
        """
        Gera leitura do sensor.

        Args:
            timestamp (datetime): Momento da leitura

        Returns:
            dict: Leitura do sensor
        """
        # Simulação de variação gradual com ruído
        drift = (random.random() - 0.5) * 0.1 * (self.max_value - self.min_value)
        self.current_value += drift

        # Garantir limites
        self.current_value = max(self.min_value,
                                min(self.max_value, self.current_value))

        # Adicionar ruído
        noise = (random.random() - 0.5) * 2 * self.noise_factor * self.current_value
        reading_value = self.current_value + noise

        return {
            'timestamp': timestamp.isoformat(),
            'sensor': self.name,
            'value': round(reading_value, 2),
            'unit': self.unit
        }


class EnvironmentalSensor:
    """
    Simula sensores ambientais (umidade, temperatura, etc.).
    """

    def __init__(self, name, min_value, max_value, unit, daily_pattern=False):
        """
        Inicializa sensor ambiental.

        Args:
            name (str): Nome do sensor
            min_value (float): Valor mínimo possível
            max_value (float): Valor máximo possível
            unit (str): Unidade de medida
            daily_pattern (bool): Indica se sensor segue padrão diário
        """
        self.name = name
        self.min_value = min_value
        self.max_value = max_value
        self.unit = unit
        self.daily_pattern = daily_pattern

    def read(self, timestamp):
        """
        Gera leitura do sensor.

        Args:
            timestamp (datetime): Momento da leitura

        Returns:
            dict: Leitura do sensor
        """
        if self.daily_pattern:
            # Simular padrão diário (ex: temperatura mais alta meio-dia)
            hour = timestamp.hour + timestamp.minute / 60.0
            daily_factor = math.sin((hour - 6) * math.pi / 12) * 0.5 + 0.5
            daily_factor = max(0, min(1, daily_factor))

            range_size = self.max_value - self.min_value
            base_value = self.min_value + range_size * daily_factor
        else:
            # Valor aleatório dentro do intervalo
            base_value = random.uniform(self.min_value, self.max_value)

        # Adicionar variação aleatória
        noise = (random.random() - 0.5) * 0.1 * (self.max_value - self.min_value)
        reading_value = base_value + noise

        return {
            'timestamp': timestamp.isoformat(),
            'sensor': self.name,
            'value': round(reading_value, 2),
            'unit': self.unit
        }


class EmissionsSensor:
    """
    Simula sensores de emissões de GHG.
    """

    def __init__(self, name, base_value, unit, sensitivity=None):
        """
        Inicializa sensor de emissões.

        Args:
            name (str): Nome do sensor
            base_value (float): Valor base de emissão
            unit (str): Unidade de medida
            sensitivity (dict): Sensibilidade a outros sensores
        """
        self.name = name
        self.base_value = base_value
        self.unit = unit
        self.sensitivity = sensitivity or {}

    def read(self, timestamp, operational_readings, environmental_readings):
        """
        Gera leitura do sensor baseada em outros sensores.

        Args:
            timestamp (datetime): Momento da leitura
            operational_readings (dict): Leituras de sensores operacionais
            environmental_readings (dict): Leituras de sensores ambientais

        Returns:
            dict: Leitura do sensor
        """
        emission_value = self.base_value

        # Aplicar fatores de sensibilidade a leituras operacionais
        for sensor_name, factor in self.sensitivity.get('operational', {}).items():
            if sensor_name in operational_readings:
                sensor_reading = operational_readings[sensor_name]['value']
                emission_value += sensor_reading * factor

        # Aplicar fatores de sensibilidade a leituras ambientais
        for sensor_name, factor in self.sensitivity.get('environmental', {}).items():
            if sensor_name in environmental_readings:
                sensor_reading = environmental_readings[sensor_name]['value']
                emission_value += sensor_reading * factor

        # Garantir valor não-negativo
        emission_value = max(0, emission_value)

        # Adicionar variação aleatória
        noise = (random.random() - 0.5) * 0.2 * emission_value
        reading_value = emission_value + noise

        return {
            'timestamp': timestamp.isoformat(),
            'sensor': self.name,
            'value': round(reading_value, 3),
            'unit': self.unit
        }
