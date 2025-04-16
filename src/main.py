#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 5. Arquivo Principal
# src/main.py
import os
import sys
import json
from datetime import datetime

# Importa componentes do sistema
from simulation.sensor_simulator import SensorSimulator
from simulation.weather_simulator import WeatherSimulator
from ghg_inventory.boundary_manager import BoundaryManager
from ghg_inventory.emissions_calculator import EmissionsCalculator
from ghg_inventory.carbon_stock_manager import CarbonStockManager
from ghg_inventory.reporting_engine import ReportingEngine
from processing.data_analyzer import DataAnalyzer
from processing.recommendation_engine import RecommendationEngine
from persistence.json_manager import JsonManager
from persistence.oracle_connector import OracleConnector
from ui.command_interface import CommandInterface

def load_config(config_file):
    """
    Carrega arquivo de configuração.

    Args:
        config_file (str): Caminho para arquivo de configuração

    Returns:
        dict: Configurações carregadas ou configuração padrão
    """
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Retorna configuração padrão
        return {
            "simulation": {
                "region": "southeast_brazil",
                "season": "harvest",
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
                        },
                        "nh3_emission": {
                            "base_value": 0.3,
                            "unit": "kg/h",
                            "sensitivity": {
                                "environmental": {
                                    "soil_humidity": 0.4
                                }
                            }
                        }
                    }
                }
            },
            "ghg_inventory": {
                "organizational_boundary": "operational_control",
                "include_scope3": False,
                "base_period": "2025",
                "track_soil_carbon": True,
                "track_above_ground": True,
                "track_below_ground": True,
                "track_dom": True,
                "amortization_period": 20,
                "calculation_tier": "tier1"
            },
            "oracle": {
                "host": "localhost",
                "port": 1521,
                "service_name": "ORCL",
                "username": "system",
                "password": "oracle",
                "simulated_mode": True
            }
        }

def setup_directories():
    """
    Configura estrutura de diretórios.

    Returns:
        bool: Sucesso da operação
    """
    directories = [
        "data",
        "data/sessions",
        "data/sensor_data",
        "data/ghg_inventory",
        "data/analysis",
        "data/recommendations"
    ]

    try:
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
        return True
    except Exception as e:
        print(f"Erro ao criar diretórios: {str(e)}")
        return False

def initialize_components(config):
    """
    Inicializa componentes do sistema.

    Args:
        config (dict): Configurações do sistema

    Returns:
        dict: Componentes inicializados
    """
    components = {}

    # Inicializa componentes de persistência
    components['json_manager'] = JsonManager(config.get('json', {}))
    components['oracle_connector'] = OracleConnector(config.get('oracle', {}))

    # Inicializa componentes de simulação
    components['sensor_simulator'] = SensorSimulator(config.get('simulation', {}))
    components['weather_simulator'] = WeatherSimulator(config.get('simulation', {}))

    # Inicializa componentes de inventário GHG
    ghg_config = config.get('ghg_inventory', {})
    components['boundary_manager'] = BoundaryManager(ghg_config)

    # Carrega fatores de emissão
    emission_factors = {
        'diesel': {
            'CO2': 2.68,
            'CH4': 0.0001,
            'N2O': 0.0001,
            'CO2e': 2.71
        },
        'fertilizer_n': {
            'N2O': 0.01,
            'CO2e': 4.87
        }
    }

    components['emissions_calculator'] = EmissionsCalculator(
        components['boundary_manager'],
        emission_factors
    )

    components['carbon_stock_manager'] = CarbonStockManager(ghg_config)

    components['reporting_engine'] = ReportingEngine(
        components['boundary_manager'],
        components['emissions_calculator'],
        components['carbon_stock_manager']
    )

    # Inicializa componentes de processamento
    components['data_analyzer'] = DataAnalyzer(config.get('analysis', {}))
    components['recommendation_engine'] = RecommendationEngine(
        config.get('recommendations', {})
    )

    # Inicializa interface de comando
    components['command_interface'] = CommandInterface(config.get('ui', {}))

    return components

def main():
    """
    Função principal do sistema.
    """
    print("Inicializando sistema...")

    # Configura diretórios
    if not setup_directories():
        print("Erro ao configurar diretórios. Encerrando programa.")
        sys.exit(1)

    # Carrega configurações
    config_file = "data/config.json"
    config = load_config(config_file)

    # Inicializa componentes
    components = initialize_components(config)

    # Configura interface de comando
    command_interface = components.pop('command_interface')
    command_interface.set_components(components)

    # Inicia interface
    command_interface.start()

if __name__ == "__main__":
    main()