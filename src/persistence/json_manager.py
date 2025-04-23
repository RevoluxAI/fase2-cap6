#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Sistema de Persistência
import os
import json
import uuid
from datetime import datetime

class JsonManager:
    """
    Gerencia persistência de dados em arquivos JSON.
    """

    def __init__(self, config=None):
        """
        Inicializa o gerenciador de arquivos JSON.

        Args:
            config (dict): Configurações para persistência JSON
        """
        self.config = config or {}
        self.base_path = self.config.get('json_base_path', 'data')

        # Estrutura de diretórios
        self.data_dirs = {
            'sessions': os.path.join(self.base_path, 'sessions'),
            'sensor_data': os.path.join(self.base_path, 'sensor_data'),
            'ghg_inventory': os.path.join(self.base_path, 'ghg_inventory'),
            'analysis': os.path.join(self.base_path, 'analysis'),
            'recommendations': os.path.join(self.base_path, 'recommendations'),
            'config': self.base_path
        }

        # Garante que diretórios existam
        self._ensure_directories()

        # Identificador da sessão atual
        self.current_session_id = None

    def _ensure_directories(self):
        """
        Garante que os diretórios necessários existam.
        """
        for directory in self.data_dirs.values():
            if not os.path.exists(directory):
                os.makedirs(directory)

    def create_session(self):
        """
        Cria uma nova sessão de coleta de dados.

        Returns:
            str: Identificador da sessão
        """
        # Gera ID único para sessão
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        session_id = f"{timestamp}-{str(uuid.uuid4())[:8]}"
        self.current_session_id = session_id

        # Cria arquivo da sessão
        session_data = {
            'session_id': session_id,
            'start_timestamp': datetime.now().isoformat(),
            'end_timestamp': None,
            'status': 'active'
        }

        session_file = os.path.join(
            self.data_dirs['sessions'],
            f"{session_id}.json"
        )

        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)

        return session_id

    def end_session(self, session_id=None):
        """
        Finaliza uma sessão de coleta de dados.

        Args:
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        session_id = session_id or self.current_session_id
        if not session_id:
            return False

        # Atualiza arquivo da sessão
        session_file = os.path.join(
            self.data_dirs['sessions'],
            f"{session_id}.json"
        )

        if not os.path.exists(session_file):
            return False

        with open(session_file, 'r') as f:
            session_data = json.load(f)

        session_data['end_timestamp'] = datetime.now().isoformat()
        session_data['status'] = 'completed'

        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)

        return True

    def save_sensor_data(self, data, session_id=None):
        """
        Salva dados de sensores em arquivo JSON.

        Args:
            data (dict): Dados dos sensores
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        session_id = session_id or self.current_session_id
        if not session_id:
            return False

        # Define arquivo para salvar
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        sensor_file = os.path.join(
            self.data_dirs['sensor_data'],
            f"{session_id}-{timestamp}.json"
        )

        # Adiciona metadados
        data_with_meta = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }

        with open(sensor_file, 'w') as f:
            json.dump(data_with_meta, f, indent=2)

        return True

    def save_ghg_inventory(self, inventory_data, session_id=None):
        """
        Salva inventário GHG em arquivo JSON.

        Args:
            inventory_data (dict): Dados do inventário GHG
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        session_id = session_id or self.current_session_id
        if not session_id:
            return False

        # Define arquivo para salvar
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        inventory_file = os.path.join(
            self.data_dirs['ghg_inventory'],
            f"{session_id}-{timestamp}.json"
        )

        # Adiciona metadados
        data_with_meta = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'inventory': inventory_data
        }

        with open(inventory_file, 'w') as f:
            json.dump(data_with_meta, f, indent=2)

        return True

    def save_analysis_results(self, analysis_data, session_id=None):
        """
        Salva resultados de análise em arquivo JSON.

        Args:
            analysis_data (dict): Resultados da análise
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        session_id = session_id or self.current_session_id
        if not session_id:
            return False

        # Define arquivo para salvar
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        analysis_file = os.path.join(
            self.data_dirs['analysis'],
            f"{session_id}-{timestamp}.json"
        )

        # Adiciona metadados
        data_with_meta = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'analysis': analysis_data
        }

        with open(analysis_file, 'w') as f:
            json.dump(data_with_meta, f, indent=2)

        return True

    def save_recommendations(self, recommendations_data, session_id=None):
        """
        Salva recomendações em arquivo JSON.

        Args:
            recommendations_data (dict): Recomendações geradas
            session_id (str): Identificador da sessão

        Returns:
            bool: Sucesso da operação
        """
        session_id = session_id or self.current_session_id
        if not session_id:
            return False

        # Define arquivo para salvar
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        recommendations_file = os.path.join(
            self.data_dirs['recommendations'],
            f"{session_id}-{timestamp}.json"
        )

        # Adiciona metadados
        data_with_meta = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'recommendations': recommendations_data
        }

        with open(recommendations_file, 'w') as f:
            json.dump(data_with_meta, f, indent=2)

        return True

    def load_session_data(self, session_id):
        """
        Carrega dados completos de uma sessão.

        Args:
            session_id (str): Identificador da sessão

        Returns:
            dict: Dados consolidados da sessão
        """
        if not session_id:
            return None

        session_file = os.path.join(
            self.data_dirs['sessions'],
            f"{session_id}.json"
        )

        if not os.path.exists(session_file):
            return None

        with open(session_file, 'r') as f:
            session_data = json.load(f)

        return session_data

    def list_sessions(self):
        """
        Lista todas as sessões disponíveis.

        Returns:
            list: Lista de identificadores de sessão
        """
        sessions = []

        for filename in os.listdir(self.data_dirs['sessions']):
            if filename.endswith('.json'):
                session_id = filename.replace('.json', '')
                sessions.append(session_id)

        return sessions

    def load_config(self, config_name):
        """
        Carrega arquivo de configuração.

        Args:
            config_name (str): Nome do arquivo de configuração

        Returns:
            dict: Dados de configuração
        """
        config_file = os.path.join(self.data_dirs['config'], f"{config_name}.json")

        if not os.path.exists(config_file):
            return None

        with open(config_file, 'r') as f:
            config_data = json.load(f)

        return config_data

    def save_config(self, config_name, config_data):
        """
        Salva arquivo de configuração.

        Args:
            config_name (str): Nome do arquivo de configuração
            config_data (dict): Dados de configuração

        Returns:
            bool: Sucesso da operação
        """
        config_file = os.path.join(self.data_dirs['config'], f"{config_name}.json")

        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)

        return True
