#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 6.4. Testes Unitários
# tests/test_json_manager.py
import unittest
import os
import tempfile
import json
import shutil
from src.persistence.json_manager import JsonManager

class TestJsonManager(unittest.TestCase):
    """
    Testes para o gerenciador de persistência JSON.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        # Cria diretório temporário para testes
        self.test_dir = tempfile.mkdtemp()

        # Configura gerenciador para usar diretório de teste
        self.config = {
            'json_base_path': self.test_dir
        }

        self.manager = JsonManager(self.config)

    def tearDown(self):
        """
        Limpa recursos após testes.
        """
        # Remove diretório temporário
        shutil.rmtree(self.test_dir)

    def test_session_lifecycle(self):
        """
        Testa ciclo de vida completo de uma sessão.
        """
        # Cria sessão
        session_id = self.manager.create_session()

        # Verifica se sessão foi criada
        self.assertIsNotNone(session_id)

        # Verifica se arquivo da sessão existe
        session_file = os.path.join(
            self.manager.data_dirs['sessions'],
            f"{session_id}.json"
        )
        self.assertTrue(os.path.exists(session_file))

        # Verifica conteúdo do arquivo
        with open(session_file, 'r') as f:
            session_data = json.load(f)

        self.assertEqual(session_data['session_id'], session_id)
        self.assertEqual(session_data['status'], 'active')
        self.assertIsNone(session_data['end_timestamp'])

        # Finaliza sessão
        result = self.manager.end_session(session_id)
        self.assertTrue(result)

        # Verifica se status foi atualizado
        with open(session_file, 'r') as f:
            session_data = json.load(f)

        self.assertEqual(session_data['status'], 'completed')
        self.assertIsNotNone(session_data['end_timestamp'])

    def test_save_sensor_data(self):
        """
        Testa salvamento de dados de sensores.
        """
        # Cria sessão
        session_id = self.manager.create_session()

        # Dados de teste
        sensor_data = {
            'harvester_speed': {
                'timestamp': '2025-04-15T10:30:00',
                'value': 5.5,
                'unit': 'km/h'
            },
            'soil_humidity': {
                'timestamp': '2025-04-15T10:30:00',
                'value': 35.0,
                'unit': '%'
            }
        }

        # Salva dados
        result = self.manager.save_sensor_data(sensor_data, session_id)
        self.assertTrue(result)

        # Verifica se arquivo foi criado
        sensor_files = os.listdir(self.manager.data_dirs['sensor_data'])
        self.assertGreaterEqual(len(sensor_files), 1)

        # Verifica conteúdo do arquivo
        sensor_file = os.path.join(
            self.manager.data_dirs['sensor_data'],
            sensor_files[0]
        )

        with open(sensor_file, 'r') as f:
            saved_data = json.load(f)

        self.assertEqual(saved_data['session_id'], session_id)
        self.assertEqual(saved_data['data'], sensor_data)

    def test_list_sessions(self):
        """
        Testa listagem de sessões.
        """
        # Cria múltiplas sessões
        session1 = self.manager.create_session()
        session2 = self.manager.create_session()

        # Lista sessões
        sessions = self.manager.list_sessions()

        # Verifica se sessões criadas estão na lista
        self.assertIn(session1, sessions)
        self.assertIn(session2, sessions)