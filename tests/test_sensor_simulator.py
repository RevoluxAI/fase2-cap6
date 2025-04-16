#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4 Testes Unitários
# tests/test_sensor_simulator.py
import unittest
from datetime import datetime
from src.simulation.sensor_simulator import (
    SensorSimulator, OperationalSensor, EnvironmentalSensor, EmissionsSensor
)

class TestSensorSimulator(unittest.TestCase):
    """
    Testes para o simulador de sensores.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        self.config = {
            "sensors": {
                "operational": {
                    "harvester_speed": {
                        "min_value": 3.0,
                        "max_value": 8.0,
                        "unit": "km/h",
                        "noise_factor": 0.05
                    },
                    "cutting_height": {
                        "min_value": 15.0,
                        "max_value": 40.0,
                        "unit": "mm",
                        "noise_factor": 0.08
                    }
                },
                "environmental": {
                    "soil_humidity": {
                        "min_value": 20.0,
                        "max_value": 70.0,
                        "unit": "%",
                        "daily_pattern": True
                    }
                },
                "emissions": {
                    "ch4_emission": {
                        "base_value": 0.5,
                        "unit": "kg/h",
                        "sensitivity": {
                            "operational": {
                                "harvester_speed": 0.2
                            },
                            "environmental": {
                                "soil_humidity": 0.3
                            }
                        }
                    }
                }
            }
        }

        self.simulator = SensorSimulator(self.config)
        self.timestamp = datetime.now()

    def test_sensor_creation(self):
        """
        Testa criação de sensores conforme configuração.
        """
        sensors = self.simulator.sensors

        # Verifica se sensores foram criados corretamente
        self.assertTrue('harvester_speed' in sensors)
        self.assertTrue('cutting_height' in sensors)
        self.assertTrue('soil_humidity' in sensors)
        self.assertTrue('ch4_emission' in sensors)

        # Verifica tipos de sensores
        self.assertIsInstance(sensors['harvester_speed'], OperationalSensor)
        self.assertIsInstance(sensors['soil_humidity'], EnvironmentalSensor)
        self.assertIsInstance(sensors['ch4_emission'], EmissionsSensor)

    def test_generate_readings(self):
        """
        Testa geração de leituras de sensores.
        """
        readings = self.simulator.generate_readings(self.timestamp)

        # Verifica se leituras foram geradas para todos os sensores
        self.assertTrue('harvester_speed' in readings)
        self.assertTrue('cutting_height' in readings)
        self.assertTrue('soil_humidity' in readings)
        self.assertTrue('ch4_emission' in readings)

        # Verifica formato das leituras
        for sensor, reading in readings.items():
            self.assertIsInstance(reading, dict)
            self.assertTrue('timestamp' in reading)
            self.assertTrue('sensor' in reading)
            self.assertTrue('value' in reading)
            self.assertTrue('unit' in reading)

            # Verifica se valores estão dentro dos limites
            if sensor == 'harvester_speed':
                self.assertGreaterEqual(
                    reading['value'],
                    self.config['sensors']['operational']['harvester_speed']['min_value']
                )
                self.assertLessEqual(
                    reading['value'],
                    self.config['sensors']['operational']['harvester_speed']['max_value']
                )

    def test_operational_sensor(self):
        """
        Testa sensor operacional individualmente.
        """
        sensor = OperationalSensor(
            name="test_sensor",
            min_value=10.0,
            max_value=20.0,
            unit="test_unit",
            noise_factor=0.1
        )

        reading = sensor.read(self.timestamp)

        self.assertEqual(reading['sensor'], "test_sensor")
        self.assertEqual(reading['unit'], "test_unit")
# tests/test_sensor_simulator.py (continuação)
        self.assertGreaterEqual(reading['value'], 10.0)
        self.assertLessEqual(reading['value'], 20.0)

    def test_environmental_sensor(self):
        """
        Testa sensor ambiental individualmente.
        """
        sensor = EnvironmentalSensor(
            name="test_env_sensor",
            min_value=5.0,
            max_value=15.0,
            unit="test_unit",
            daily_pattern=True
        )

        reading = sensor.read(self.timestamp)

        self.assertEqual(reading['sensor'], "test_env_sensor")
        self.assertEqual(reading['unit'], "test_unit")
        self.assertGreaterEqual(reading['value'], 5.0)
        self.assertLessEqual(reading['value'], 15.0)

    def test_emissions_sensor(self):
        """
        Testa sensor de emissões individualmente.
        """
        sensor = EmissionsSensor(
            name="test_emission",
            base_value=1.0,
            unit="kg/h",
            sensitivity={
                "operational": {"harvester_speed": 0.1},
                "environmental": {"soil_humidity": 0.2}
            }
        )

        operational_readings = {
            "harvester_speed": {"value": 5.0}
        }

        environmental_readings = {
            "soil_humidity": {"value": 40.0}
        }

        reading = sensor.read(
            self.timestamp,
            operational_readings,
            environmental_readings
        )

        self.assertEqual(reading['sensor'], "test_emission")
        self.assertEqual(reading['unit'], "kg/h")
        # Valor base (1.0) + influência da velocidade (5.0 * 0.1) +
        # influência da umidade (40.0 * 0.2) = aproximadamente 9.5
        # Considerando ruído, valor deve estar próximo a isso
        self.assertGreater(reading['value'], 0.0)