#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 4. Interface de Linha de Comando
# src/ui/command_interface.py
import os
import time
from datetime import datetime, timedelta

class CommandInterface:
    """
    Interface de linha de comando para o sistema.
    """

    def __init__(self, config=None):
        """
        Inicializa a interface de comando.

        Args:
            config (dict): Configurações da interface
        """
        self.config = config or {}
        self.title = self.config.get(
            'title',
            'Sistema de Redução de Perdas na Colheita Mecanizada de Cana-de-Açúcar'
        )
        self.width = self.config.get('width', 80)
        self.session_active = False
        self.session_id = None

        # Componentes do sistema
        self.sensor_simulator = None
        self.weather_simulator = None
        self.boundary_manager = None
        self.emissions_calculator = None
        self.carbon_stock_manager = None
        self.reporting_engine = None
        self.data_analyzer = None
        self.recommendation_engine = None
        self.json_manager = None
        self.oracle_connector = None

    def set_components(self, components):
        """
        Define componentes do sistema.

        Args:
            components (dict): Componentes do sistema
        """
        self.sensor_simulator = components.get('sensor_simulator')
        self.weather_simulator = components.get('weather_simulator')
        self.boundary_manager = components.get('boundary_manager')
        self.emissions_calculator = components.get('emissions_calculator')
        self.carbon_stock_manager = components.get('carbon_stock_manager')
        self.reporting_engine = components.get('reporting_engine')
        self.data_analyzer = components.get('data_analyzer')
        self.recommendation_engine = components.get('recommendation_engine')
        self.json_manager = components.get('json_manager')
        self.oracle_connector = components.get('oracle_connector')

    def _clear_screen(self):
        """
        Limpa a tela do terminal.
        """
        os.system('cls' if os.name == 'nt' else 'clear')

    def _print_header(self, subtitle=None):
        """
        Imprime cabeçalho do sistema.

        Args:
            subtitle (str): Subtítulo opcional
        """
        self._clear_screen()
        print('=' * self.width)
        print(self.title.center(self.width))
        if subtitle:
            print(subtitle.center(self.width))
        print('=' * self.width)
        if self.session_active:
            print(f"Sessão ativa: {self.session_id}")
            print('-' * self.width)

    def _print_section(self, title):
        """
        Imprime título de seção.

        Args:
            title (str): Título da seção
        """
        print('\n' + '-' * self.width)
        print(title.center(self.width))
        print('-' * self.width)

    def _input_with_prompt(self, prompt, default=None):
        """
        Solicita entrada do usuário com prompt.

        Args:
            prompt (str): Texto do prompt
            default (str): Valor padrão se usuário não digitar nada

        Returns:
            str: Entrada do usuário
        """
        if default:
            prompt = f"{prompt} [{default}]: "
        else:
            prompt = f"{prompt}: "

        user_input = input(prompt)

        if not user_input and default is not None:
            return default

        return user_input

    def _wait_keypress(self):
        """
        Aguarda pressionar tecla para continuar.
        """
        input("\nPressione ENTER para continuar...")

    def start(self):
        """
        Inicia a interface de comando.
        """
        while True:
            self._print_header()

            if not self.session_active:
                self._show_main_menu()
            else:
                self._show_session_menu()

    def _show_main_menu(self):
        """
        Exibe menu principal.
        """
        print("\nMENU PRINCIPAL")
        print("1. Iniciar nova sessão")
        print("2. Carregar sessão existente")
        print("3. Configurações")
        print("4. Sair")

        choice = self._input_with_prompt("\nEscolha uma opção", "1")

        if choice == '1':
            self._start_new_session()
        elif choice == '2':
            self._load_session()
        elif choice == '3':
            self._show_config_menu()
        elif choice == '4':
            self._exit_program()
        else:
            print("Opção inválida!")
            time.sleep(1)

    def _show_session_menu(self):
        """
        Exibe menu de sessão ativa.
        """
        print("\nMENU DE SESSÃO")
        print("1. Iniciar simulação de colheita")
        print("2. Visualizar dados de sensores")
        print("3. Analisar perdas e emissões")
        print("4. Ver recomendações")
        print("5. Gerar inventário GHG")
        print("6. Salvar dados no Oracle")
        print("7. Encerrar sessão")
        print("8. Voltar ao menu principal")

        choice = self._input_with_prompt("\nEscolha uma opção", "1")

        if choice == '1':
            self._run_harvest_simulation()
        elif choice == '2':
            self._view_sensor_data()
        elif choice == '3':
            self._analyze_data()
        elif choice == '4':
            self._view_recommendations()
        elif choice == '5':
            self._generate_ghg_inventory()
        elif choice == '6':
            self._save_to_oracle()
        elif choice == '7':
            self._end_session()
        elif choice == '8':
            self._exit_session()
        else:
            print("Opção inválida!")
            time.sleep(1)

    def _show_config_menu(self):
        """
        Exibe menu de configurações.
        """
        self._print_header("Configurações")

        print("\nCONFIGURAÇÕES")
        print("1. Configurar parâmetros de simulação")
        print("2. Configurar limites de inventário GHG")
        print("3. Configurar conexão Oracle")
        print("4. Voltar ao menu principal")

        choice = self._input_with_prompt("\nEscolha uma opção", "4")

        if choice == '1':
            self._configure_simulation()
        elif choice == '2':
            self._configure_ghg_boundaries()
        elif choice == '3':
            self._configure_oracle()
        elif choice == '4':
            return
        else:
            print("Opção inválida!")
            time.sleep(1)

    def _start_new_session(self):
        """
        Inicia nova sessão de coleta.
        """
        self._print_header("Nova Sessão")

        if not self.json_manager:
            print("Erro: Gerenciador JSON não inicializado!")
            self._wait_keypress()
            return

        # Cria nova sessão
        session_id = self.json_manager.create_session()

        print(f"\nNova sessão criada com ID: {session_id}")
        print("\nInicializando componentes do sistema...")

        # Configura sessão atual
        self.session_id = session_id
        self.session_active = True

        # Inicializa banco Oracle se configurado
        if self.oracle_connector:
            print("Conectando ao Oracle...")
            if self.oracle_connector.connect():
                print("Criando tabelas no Oracle...")
                self.oracle_connector.create_tables()
                print("Registrando sessão no Oracle...")
                self.oracle_connector.start_session(session_id)

        print("\nSessão inicializada com sucesso!")
        self._wait_keypress()

    def _load_session(self):
        """
        Carrega sessão existente.
        """
        self._print_header("Carregar Sessão")

        if not self.json_manager:
            print("Erro: Gerenciador JSON não inicializado!")
            self._wait_keypress()
            return

        # Lista sessões disponíveis
        sessions = self.json_manager.list_sessions()

        if not sessions:
            print("\nNenhuma sessão encontrada!")
            self._wait_keypress()
            return

        print("\nSessões disponíveis:")
        for i, session_id in enumerate(sessions, 1):
            session_data = self.json_manager.load_session_data(session_id)
            status = session_data.get('status', 'unknown')
            start = session_data.get('start_timestamp', 'unknown')

            print(f"{i}. {session_id} - Status: {status} - Iniciada em: {start}")

        choice = self._input_with_prompt(
            "\nEscolha uma sessão (número) ou 0 para cancelar",
            "0"
        )

        try:
            choice_idx = int(choice)
            if choice_idx == 0:
                return

            if 1 <= choice_idx <= len(sessions):
                selected_session = sessions[choice_idx - 1]
                self.session_id = selected_session
                self.session_active = True

                print(f"\nSessão {selected_session} carregada com sucesso!")
                self._wait_keypress()
            else:
                print("\nOpção inválida!")
                self._wait_keypress()
        except ValueError:
            print("\nOpção inválida!")
            self._wait_keypress()

    def _configure_simulation(self):
        """
        Configura parâmetros de simulação.
        """
        self._print_header("Configuração de Simulação")

        print("\nConfiguração de parâmetros de simulação")

        # Exemplo de configuração
        sim_config = {}

        # Configuração região e estação
        sim_config['region'] = self._input_with_prompt(
            "Região (southeast_brazil, northeast_brazil, central_brazil)",
            "southeast_brazil"
        )

        sim_config['season'] = self._input_with_prompt(
            "Estação (harvest, growth, planting)",
            "harvest"
        )

        # Configuração de sensores
        sim_config['sensors'] = {
            'operational': {
                'harvester_speed': {
                    'min_value': float(self._input_with_prompt(
                        "Velocidade mínima da colheitadeira (km/h)",
                        "3.0"
                    )),
                    'max_value': float(self._input_with_prompt(
                        "Velocidade máxima da colheitadeira (km/h)",
                        "8.0"
                    )),
                    'unit': 'km/h'
                },
                'cutting_height': {
                    'min_value': float(self._input_with_prompt(
                        "Altura mínima de corte (mm)",
                        "15.0"
                    )),
                    'max_value': float(self._input_with_prompt(
                        "Altura máxima de corte (mm)",
                        "40.0"
                    )),
                    'unit': 'mm'
                }
            }
        }

        # Salva configuração
        if self.json_manager:
            self.json_manager.save_config('simulation', sim_config)
            print("\nConfiguração salva com sucesso!")
        else:
            print("\nErro: Não foi possível salvar a configuração!")

        self._wait_keypress()

    def _configure_ghg_boundaries(self):
        """
        Configura limites de inventário GHG.
        """
        self._print_header("Configuração de Limites GHG")

        print("\nConfiguração de limites do inventário GHG")

        # Exemplo de configuração
        ghg_config = {}

        # Configuração de limites organizacionais
        print("\nLimites Organizacionais:")
        print("1. Controle Operacional")
        print("2. Controle Financeiro")
        print("3. Participação Acionária")

        org_choice = self._input_with_prompt("Escolha o limite organizacional", "1")

        if org_choice == '1':
            ghg_config['organizational_boundary'] = 'operational_control'
        elif org_choice == '2':
            ghg_config['organizational_boundary'] = 'financial_control'
        elif org_choice == '3':
            ghg_config['organizational_boundary'] = 'equity_share'
        else:
            ghg_config['organizational_boundary'] = 'operational_control'

        # Configuração de limites operacionais
        print("\nLimites Operacionais:")
        ghg_config['include_scope3'] = self._input_with_prompt(
            "Incluir Escopo 3? (s/n)",
            "n"
        ).lower() == 's'

        # Configuração de estoques de carbono
        print("\nEstoques de Carbono:")
        ghg_config['track_soil_carbon'] = self._input_with_prompt(
            "Monitorar carbono no solo? (s/n)",
            "s"
        ).lower() == 's'

        ghg_config['track_above_ground'] = self._input_with_prompt(
            "Monitorar biomassa acima do solo? (s/n)",
            "s"
        ).lower() == 's'

        ghg_config['track_below_ground'] = self._input_with_prompt(
            "Monitorar biomassa abaixo do solo? (s/n)",
            "s"
        ).lower() == 's'

        ghg_config['amortization_period'] = int(self._input_with_prompt(
            "Período de amortização (anos)",
            "20"
        ))

        # Salva configuração
        if self.json_manager:
            self.json_manager.save_config('ghg_inventory', ghg_config)
            print("\nConfiguração salva com sucesso!")
        else:
            print("\nErro: Não foi possível salvar a configuração!")

        self._wait_keypress()

    def _configure_oracle(self):
        """
        Configura conexão com Oracle.
        """
        self._print_header("Configuração Oracle")

        print("\nConfiguração de conexão com Oracle")

        # Exemplo de configuração
        oracle_config = {}

        oracle_config['host'] = self._input_with_prompt(
            "Host do servidor Oracle",
            "localhost"
        )

        oracle_config['port'] = int(self._input_with_prompt("Porta", "1521"))

        oracle_config['service_name'] = self._input_with_prompt(
            "Nome do serviço",
            "ORCL"
        )

        oracle_config['username'] = self._input_with_prompt("Usuário", "system")

        oracle_config['password'] = self._input_with_prompt("Senha", "oracle")

        oracle_config['simulated_mode'] = self._input_with_prompt(
            "Modo simulado (sem conexão real)? (s/n)",
            "s"
        ).lower() == 's'

        # Salva configuração
        if self.json_manager:
            self.json_manager.save_config('oracle', oracle_config)
            print("\nConfiguração salva com sucesso!")
        else:
            print("\nErro: Não foi possível salvar a configuração!")

        self._wait_keypress()

    def _run_harvest_simulation(self):
        """
        Executa simulação de colheita.
        """
        self._print_header("Simulação de Colheita")

        if not all([self.sensor_simulator, self.weather_simulator,
                  self.data_analyzer, self.recommendation_engine,
                  self.json_manager]):
            print("Erro: Componentes necessários não inicializados!")
            self._wait_keypress()
            return

        # Define parâmetros da simulação
        duration = int(self._input_with_prompt(
            "Duração da simulação (minutos)",
            "5"
        ))

        interval = int(self._input_with_prompt(
            "Intervalo entre leituras (segundos)",
            "10"
        ))

        steps = int(duration * 60 / interval)

        print(f"\nIniciando simulação com {steps} passos...")
        print("Pressione Ctrl+C para interromper\n")

        try:
            start_time = datetime.now()

            for step in range(1, steps + 1):
                # Calcula timestamp simulado
                current_time = start_time + timedelta(seconds=step * interval)

                # Simula condições climáticas
                weather_data = self.weather_simulator.get_weather_conditions(
                    current_time
                )

                # Simula leituras de sensores
                sensor_data = self.sensor_simulator.generate_readings(current_time)

                # Combina dados
                combined_data = {**sensor_data, **weather_data}

                # Analisa dados
                analysis = self.data_analyzer.process_sensor_data(
                    current_time,
                    combined_data
                )

                # Gera recomendações
                recommendations = self.recommendation_engine.generate_recommendations(
                    analysis
                )

                # Exibe resumo
                print(f"\rPasso {step}/{steps} - "
                     f"Perda estimada: {analysis['loss_estimate']:.2f}% - "
                     f"Categoria: {analysis['loss_category']}",
                     end="")

                # Salva dados
                self.json_manager.save_sensor_data(combined_data, self.session_id)
                self.json_manager.save_analysis_results(analysis, self.session_id)
                self.json_manager.save_recommendations(
                    recommendations,
                    self.session_id
                )

                # Aguarda próximo passo
                time.sleep(0.1)  # Simulação acelerada

            print("\n\nSimulação concluída com sucesso!")
        except KeyboardInterrupt:
            print("\n\nSimulação interrompida pelo usuário.")
        except Exception as e:
            print(f"\n\nErro durante simulação: {str(e)}")

        self._wait_keypress()

    def _view_sensor_data(self):
        """
        Visualiza dados de sensores.
        """
        # Implementação simplificada
        self._print_header("Visualização de Dados")
        print("\nFuncionalidade em desenvolvimento.")
        self._wait_keypress()

    def _analyze_data(self):
        """
        Analisa dados de perdas e emissões.
        """
        # Implementação simplificada
        self._print_header("Análise de Dados")
        print("\nFuncionalidade em desenvolvimento.")
        self._wait_keypress()

    def _view_recommendations(self):
        """
        Visualiza recomendações geradas.
        """
        # Implementação simplificada
        self._print_header("Recomendações")
        print("\nFuncionalidade em desenvolvimento.")
        self._wait_keypress()

    def _generate_ghg_inventory(self):
        """
        Gera inventário de emissões GHG.
        """
        # Implementação simplificada
        self._print_header("Inventário GHG")
        print("\nFuncionalidade em desenvolvimento.")
        self._wait_keypress()

    def _save_to_oracle(self):
        """
        Salva dados no banco Oracle.
        """
        # Implementação simplificada
        self._print_header("Exportação para Oracle")
        print("\nFuncionalidade em desenvolvimento.")
        self._wait_keypress()

    def _end_session(self):
        """
        Encerra sessão atual.
        """
        self._print_header("Encerramento de Sessão")

        if not self.session_active:
            print("Nenhuma sessão ativa para encerrar!")
            self._wait_keypress()
            return

        confirm = self._input_with_prompt(
            "Confirma encerramento da sessão atual? (s/n)",
            "n"
        )

        if confirm.lower() != 's':
            print("\nOperação cancelada!")
            self._wait_keypress()
            return

        # Finaliza no JSON
        if self.json_manager:
            self.json_manager.end_session(self.session_id)

        # Finaliza no Oracle
        if self.oracle_connector:
            self.oracle_connector.end_session(self.session_id)

        print(f"\nSessão {self.session_id} encerrada com sucesso!")

        self.session_active = False
        self.session_id = None

        self._wait_keypress()

    def _exit_session(self):
        """
        Sai da sessão sem encerrar.
        """
        self._print_header("Sair da Sessão")

        if not self.session_active:
            return

        confirm = self._input_with_prompt(
            "Confirma saída da sessão atual? A sessão continuará ativa. (s/n)",
            "n"
        )

        if confirm.lower() != 's':
            print("\nOperação cancelada!")
            self._wait_keypress()
            return

        self.session_active = False

    def _exit_program(self):
        """
        Encerra o programa.
        """
        self._print_header("Encerrar Programa")

        confirm = self._input_with_prompt(
            "Confirma encerramento do programa? (s/n)",
            "n"
        )

        if confirm.lower() != 's':
            print("\nOperação cancelada!")
            self._wait_keypress()
            return

        # Encerra conexões e sessões
        if self.session_active and self.json_manager:
            self.json_manager.end_session(self.session_id)

        if self.oracle_connector:
            self.oracle_connector.disconnect()

        print("\nPrograma encerrado!")
        exit(0)