#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Interface de Linha de Comando
import os
import time
import json
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


    def _format_status(self, status):
        """
        Formata status de forma consistente para exibição.
        """
        if status == "IDEAL":
            return "✓ IDEAL"
        elif status == "ALTO":
            return "▲ ALTO"
        elif status == "BAIXO":
            return "▼ BAIXO"
        elif status == "CRÍTICO":
            return "! CRÍTICO"
        elif status == "ADEQUADO":
            return "○ ADEQUADO"
        elif status == "LIMITANTE":
            return "△ LIMITANTE"
        return status


    def _show_context_help(self, context):
        """
        Exibe ajuda contextual para a tela atual.
        """
        if context == "main":
            self._print_header("Ajuda - Visualização de Dados")
            print("\nEsta tela permite acessar diferentes visualizações dos dados")
            print("coletados pelos sensores durante a simulação de colheita.")
            print("\nCATEGORIAS DE SENSORES:")
            print("• Sensores Operacionais: Monitoram a operação da colheitadeira")
            print("  Exemplos: velocidade, altura de corte")
            print("• Sensores Ambientais: Monitoram condições do ambiente")
            print("  Exemplos: temperatura, umidade, vento")
            print("• Sensores de Emissão: Monitoram emissões de GEE")
            print("  Exemplos: emissão de CH4, NH3")
            print("\nOPÇÕES DISPONÍVEIS:")
            print("1. Visualizar conjunto: Detalhes de um momento específico")
            print("2. Histórico: Evolução de um sensor ao longo do tempo")
            print("3. Tendências: Estatísticas e comportamentos gerais")
            print("4. Parâmetros críticos: Foco nos fatores que mais afetam perdas")

        elif context == "specific_data":
            # Ajuda específica para visualização de conjunto
            pass

        # Outras ajudas contextuais...

        self._wait_keypress()




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
        Visualiza dados de sensores coletados durante a simulação.

        Apresenta visão especializada para monitoramento de perdas na colheita
        mecanizada de cana-de-açúcar, com categorização por tipo de sensor
        e destaque para parâmetros críticos.
        """
        self._print_header("Visualização de Dados de Sensores")

        if not self.session_id:
            print("\nNenhuma sessão ativa para visualizar dados!")
            self._wait_keypress()
            return

        # Verifica se existem dados JSON para a sessão
        if not self.json_manager:
            print("\nErro: Gerenciador JSON não inicializado!")
            self._wait_keypress()
            return

        # Lista arquivos de dados de sensores para esta sessão
        sensor_files = []
        for filename in os.listdir(self.json_manager.data_dirs['sensor_data']):
            if filename.startswith(f"{self.session_id}-") and filename.endswith('.json'):
                sensor_files.append(filename)

        if not sensor_files:
            print("\nNenhum dado de sensor encontrado para esta sessão!")
            self._wait_keypress()
            return

        # Ordenar arquivos por timestamp (do mais recente para o mais antigo)
        sensor_files.sort(reverse=True)

        # Carrega dados para análise
        max_display = 5  # Número máximo de conjuntos de dados para exibir inicialmente
        sensor_data_list = []

        for i, filename in enumerate(sensor_files[:max_display]):
            file_path = os.path.join(self.json_manager.data_dirs['sensor_data'], filename)
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                    sensor_data_list.append(data)
                except json.JSONDecodeError:
                    print(f"Aviso: Arquivo {filename} corrompido.")

        # Carrega dados de análise para contexto
        analysis_files = []
        for filename in os.listdir(self.json_manager.data_dirs['analysis']):
            if filename.startswith(f"{self.session_id}-") and filename.endswith('.json'):
                analysis_files.append(filename)

        analysis_data_list = []
        if analysis_files:
            analysis_files.sort(reverse=True)
            for i, filename in enumerate(analysis_files[:max_display]):
                file_path = os.path.join(self.json_manager.data_dirs['analysis'], filename)
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        analysis_data_list.append(data)
                    except json.JSONDecodeError:
                        pass

        # Menu de visualização
        while True:
            self._print_header("Visualização de Dados de Sensores")

            print(f"\nDados disponíveis: {len(sensor_files)} conjuntos de leituras")
            print(f"Exibindo: {len(sensor_data_list)} conjuntos mais recentes\n")

            # Legenda para categorias
            print("LEGENDA:")
            print("• Op: Sensores Operacionais (colheitadeira, altura de corte)")
            print("• Amb: Sensores Ambientais (temperatura, umidade, vento)")
            print("• Em: Sensores de Emissão (CH4, NH3)\n")

            # Se não houver dados para exibir, retorna
            if not sensor_data_list:
                print("Nenhum dado para exibir!")
                self._wait_keypress()
                return

            # Exibe resumo categorizado dos conjuntos de dados
            print("ID | Timestamp           | Perdas    | Sensores por categoria")
            print("-" * 75)


            for i, data in enumerate(sensor_data_list):
                timestamp = data.get('timestamp', 'desconhecido')
                sensors = data.get('data', {})

                # Extrai timestamp comum dos sensores para comparação
                common_timestamp = self._get_common_sensor_timestamp(sensors)

                # Busca dado de análise correspondente para mostrar perda estimada
                loss_estimate = "N/A"
                for analysis in analysis_data_list:
                    analysis_timestamp = analysis.get('analysis', {}).get('timestamp')

                    if analysis_timestamp == common_timestamp:
                        loss_val = analysis.get('analysis', {}).get('loss_estimate')
                        if isinstance(loss_val, (int, float)):
                            loss_estimate = f"{loss_val:.1f}%"
                        break


                # Categoriza sensores
                operational_sensors = []
                environmental_sensors = []
                emission_sensors = []

                for sensor_name in sensors:
                    if sensor_name in ['harvester_speed', 'cutting_height']:
                        operational_sensors.append(sensor_name)
                    elif sensor_name in ['temperature', 'humidity', 'soil_humidity',
                                    'wind_speed', 'precipitation', 'is_raining']:
                        environmental_sensors.append(sensor_name)
                    elif 'emission' in sensor_name or sensor_name in ['ch4_emission',
                                                                    'nh3_emission']:
                        emission_sensors.append(sensor_name)

                # Resume contagens por categoria
                categories = (
                    f"Op: {len(operational_sensors)}, "
                    f"Amb: {len(environmental_sensors)}, "
                    f"Em: {len(emission_sensors)}"
                )

                print(f"{i+1:2d} | {timestamp[:19]} | {loss_estimate:<9} | {categories}")

            print("\nOPÇÕES DE VISUALIZAÇÃO:")
            print("1. Visualizar conjunto de dados específico")
            print("2. Visualizar histórico de um sensor específico")
            print("3. Visualizar tendências e estatísticas")
            print("4. Visualizar parâmetros críticos para perdas na colheita")
            print("\nOPÇÕES DE NAVEGAÇÃO:")
            print("5. Carregar mais dados")
            print("6. Ajuda contextual")
            print("7. Voltar ao menu anterior")

            choice = self._input_with_prompt("\nEscolha uma opção", "7")

            if choice == '1':
                self._view_specific_data_set(sensor_data_list, analysis_data_list)
            elif choice == '2':
                self._view_sensor_history(sensor_data_list)
            elif choice == '3':
                self._view_sensor_trends(sensor_data_list)
            elif choice == '4':
                self._view_critical_parameters(sensor_data_list, analysis_data_list)
            elif choice == '5':
                # Carrega mais dados se disponíveis
                next_batch = min(len(sensor_files) - len(sensor_data_list), max_display)

                if next_batch <= 0:
                    print("\nTodos os dados já foram carregados!")
                    self._wait_keypress()
                    continue

                start_idx = len(sensor_data_list)
                end_idx = start_idx + next_batch

                for filename in sensor_files[start_idx:end_idx]:
                    file_path = os.path.join(
                        self.json_manager.data_dirs['sensor_data'],
                        filename
                    )
                    with open(file_path, 'r') as f:
                        try:
                            data = json.load(f)
                            sensor_data_list.append(data)
                        except json.JSONDecodeError:
                            print(f"Aviso: Arquivo {filename} corrompido.")

                print(f"\nCarregados {next_batch} conjuntos adicionais de dados.")
                self._wait_keypress()
            elif choice == '6':
                self._show_context_help("main")
            elif choice == '7':
                # Volta ao menu anterior
                break
            else:
                print("\nOpção inválida!")
                self._wait_keypress()


    def _view_specific_data_set(self, sensor_data_list, analysis_data_list):
        """
        Visualiza um conjunto específico de dados de sensores com contexto
        agronômico e informações sobre perdas.

        Args:
            sensor_data_list (list): Lista de conjuntos de dados de sensores
            analysis_data_list (list): Lista de análises correspondentes
        """
        self._print_header("Visualização de Conjunto de Dados")

        # Adiciona instruções claras para o usuário
        print("\nEsta tela permite analisar detalhadamente um conjunto de leituras.")
        print("Os dados são organizados por categoria para facilitar a interpretação.")

        # Apresenta resumo dos conjuntos disponíveis
        print("\nResumo dos conjuntos disponíveis:")
        print("-" * 75)
        print("ID | Timestamp           | Parâmetros Operacionais       | "
            "Condições Ambientais")
        print("-" * 75)


        for i, data in enumerate(sensor_data_list):
            timestamp = data.get('timestamp', 'desconhecido')[:19]
            sensors = data.get('data', {})

            # Extrai valores operacionais relevantes
            harvester_speed = self._get_sensor_value(sensors, 'harvester_speed')
            cutting_height = self._get_sensor_value(sensors, 'cutting_height')
            op_params = f"Vel: {harvester_speed} km/h, Alt: {cutting_height} mm"

            # Extrai condições ambientais relevantes
            temp = self._get_sensor_value(sensors, 'temperature')
            humid = self._get_sensor_value(sensors, 'humidity')
            wind = self._get_sensor_value(sensors, 'wind_speed')
            rain = "Sim" if sensors.get('is_raining') else "Não"
            env_cond = f"T: {temp}°C, H: {humid}%, V: {wind} km/h, Chuva: {rain}"

            print(f"{i+1:2d} | {timestamp} | {op_params:<29} | {env_cond}")

        # Solicita número do conjunto
        idx = self._input_with_prompt(
            f"\nNúmero do conjunto (1-{len(sensor_data_list)}) ou 0 para cancelar",
            "0"
        )

        try:
            idx = int(idx)
            if idx == 0:
                return

            if 1 <= idx <= len(sensor_data_list):
                data_set = sensor_data_list[idx - 1]

                # Busca análise correspondente
                matching_analysis = None
                for analysis in analysis_data_list:
                    if (analysis.get('analysis', {}).get('timestamp') ==
                        data_set.get('timestamp')):
                        matching_analysis = analysis.get('analysis', {})
                        break

                self._display_sensor_set(data_set, matching_analysis)
            else:
                print("\nNúmero de conjunto inválido!")
        except ValueError:
            print("\nEntrada inválida! Digite um número.")

        self._wait_keypress()


    def _get_common_sensor_timestamp(self, sensor_data):
        """
        Extrai o timestamp comum dos dados de sensores.

        Args:
            sensor_data (dict): Dicionário com dados de sensores

        Returns:
            str: Timestamp comum ou None se não encontrado
        """
        # Verifica se existe timestamp no nível superior do objeto data
        if 'timestamp' in sensor_data:
            return sensor_data['timestamp']

        # Busca em qualquer sensor (todos compartilham o mesmo timestamp)
        for sensor_name, reading in sensor_data.items():
            if isinstance(reading, dict) and 'timestamp' in reading:
                return reading['timestamp']

        return None

    def _get_sensor_value(self, sensors, sensor_name, default="N/A"):
        """
        Obtém valor de um sensor com tratamento para diferentes formatos.

        Args:
            sensors (dict): Dicionário de sensores
            sensor_name (str): Nome do sensor
            default: Valor padrão se sensor não existir

        Returns:
            Valor do sensor formatado
        """
        if sensor_name not in sensors:
            return default

        reading = sensors[sensor_name]

        # Verifica formato da leitura (objeto ou valor simples)
        if isinstance(reading, dict) and 'value' in reading:
            value = reading.get('value')
        else:
            value = reading

        # Formata valor numérico
        if isinstance(value, (int, float)):
            return f"{value:.1f}"

        return str(value)

    def _display_sensor_set(self, data_set, analysis=None):
        """
        Exibe detalhes de um conjunto de dados de sensores com contexto agronômico.

        Args:
            data_set (dict): Conjunto de dados de sensores
            analysis (dict): Análise correspondente com informações de perdas
        """
        timestamp = data_set.get('timestamp', 'desconhecido')
        session_id = data_set.get('session_id', 'desconhecido')
        sensor_data = data_set.get('data', {})

        self._print_header(f"Dados Coletados em {timestamp[:19]}")

        # Adiciona contexto para esta visualização
        print("\nEsta tela apresenta os valores detalhados de todos os sensores")
        print("para o momento selecionado, com avaliação de status em relação às")
        print("faixas ideais para colheita mecanizada de cana-de-açúcar.\n")


        # Exibe informações de perda se disponíveis
        if analysis:
            loss_estimate = analysis.get('loss_estimate', 'N/A')
            loss_category = analysis.get('loss_category', 'N/A')

            print(f"\n=== ANÁLISE DE PERDAS ===")
            print(f"Perda estimada: {loss_estimate:.2f}%")
            print(f"Categoria: {loss_category}")

            # Exibe fatores problemáticos
            problematic_factors = analysis.get('problematic_factors', [])
            if problematic_factors:
                print("\nFatores problemáticos identificados:")
                for factor in problematic_factors:
                    factor_name = factor.get('factor', '')
                    value = factor.get('value', 'N/A')
                    severity = factor.get('severity', 0) * 100
                    direction = factor.get('direction', '')

                    optimal_range = factor.get('optimal_range', [])
                    if optimal_range and len(optimal_range) == 2:
                        range_str = f"{optimal_range[0]}-{optimal_range[1]}"
                    else:
                        range_str = "N/A"

                    print(f"• {factor_name}: {value} "
                        f"({'acima' if direction == 'above' else 'abaixo'} do ideal "
                        f"{range_str}) - Severidade: {severity:.0f}%")

        # Categoriza e exibe sensores por grupo
        print("\n=== SENSORES OPERACIONAIS ===")
        print("Sensor               | Valor      | Unidade | Faixa Ideal  | Status")
        print("-" * 75)

        # Define faixas ideais e exibe sensores operacionais
        ideal_ranges = {
            'harvester_speed': (4.5, 6.5),  # km/h
            'cutting_height': (20, 30),     # mm
        }

        for sensor_name in sorted(sensor_data.keys()):
            if sensor_name in ['harvester_speed', 'cutting_height']:
                reading = sensor_data[sensor_name]

                # Extrai valor e unidade
                if isinstance(reading, dict) and 'value' in reading:
                    value = reading.get('value', 'N/A')
                    unit = reading.get('unit', '')
                else:
                    value = reading
                    unit = 'km/h' if sensor_name == 'harvester_speed' else 'mm'

                # Verifica se está na faixa ideal
                status = "NORMAL"
                if sensor_name in ideal_ranges and isinstance(value, (int, float)):
                    min_val, max_val = ideal_ranges[sensor_name]
                    ideal_range = f"{min_val}-{max_val}"

                    if value < min_val:
                        status = "BAIXO"
                    elif value > max_val:
                        status = "ALTO"
                else:
                    ideal_range = "N/A"

                print(f"{sensor_name[:20]:<20} | {str(value)[:10]:<10} | "
                    f"{unit:<7} | {ideal_range:<12} | {status}")

        # Exibe sensores ambientais
        print("\n=== CONDIÇÕES AMBIENTAIS ===")
        print("Sensor               | Valor      | Unidade | Faixa Ideal  | Status")
        print("-" * 75)

        # Define impactos e faixas para sensores ambientais
        impact_info = {
            'temperature': {
                'ranges': [ (0, 15,   "LIMITANTE  - Baixa temperatura reduz eficiência"),
                            (15, 20,  "ADEQUADO   - Temperatura aceitável"),
                            (20, 30,  "IDEAL      - Temperatura ótima para colheita"),
                            (30, 100, "CRÍTICO    - Temperatura alta aumenta perdas")]
            },
            'humidity': {
                'ranges': [ (0, 40,   "LIMITANTE  - Umidade baixa aumenta perdas por desprendimento"),
                            (40, 60,  "IDEAL      - Umidade ideal para colheita"),
                            (60, 80,  "ADEQUA     - Umidade aceitável"),
                            (80, 100, "CRÍTICO    - Umidade alta dificulta colheita")]
            },
            'precipitation': {
                'ranges': [ (0, 0.1,  "IDEAL      - Ausência de chuva, condições ótimas para colheita"),
                            (0.1, 5,  "ADEQUADO   - Precipitação leve, colheita viável"),
                            (5, 10,   "LIMITANTE  - Precipitação moderada, aumento de perdas"),
                            (10, 20,  "CRÍTICO    - Precipitação forte, colheita comprometida"),
                            (20, 999, "IMPEDITIVO - Precipitação muito forte, suspender colheita")]
            },
            'soil_humidity': {
                'ranges': [ (0, 15,   "CRÍTICO    - Solo muito seco prejudica colheita"),
                            (15, 25,  "LIMITANTE  - Umidade abaixo do ideal"),
                            (25, 45,  "IDEAL      - Umidade ideal do solo"),
                            (45, 60,  "ADEQUADO   - Umidade alta mas aceitável"),
                            (60, 100, "CRÍTICO    - Solo encharcado impede colheita")]
            },
            'wind_speed': {
                'ranges': [ (0, 5,    "IDEAL      - Vento fraco, mínima interferência"),
                            (5, 10,   "ADEQUADO   - Vento moderado aceitável"),
                            (10, 15,  "LIMITANTE  - Vento forte aumenta perdas"),
                            (15, 100, "CRÍTICO    - Vento muito forte, alta perda")]
            }
        }

        for sensor_name in sorted(sensor_data.keys()):
            if sensor_name in ['temperature', 'humidity', 'soil_humidity',
                            'wind_speed', 'precipitation']:
                reading = sensor_data[sensor_name]

                # Extrai valor e unidade
                if isinstance(reading, dict) and 'value' in reading:
                    value = reading.get('value', 'N/A')
                    unit = reading.get('unit', '')
                else:
                    value = reading
                    unit = '°C' if sensor_name == 'temperature' else \
                        '%' if sensor_name in ['humidity', 'soil_humidity'] else \
                        'km/h' if sensor_name == 'wind_speed' else 'mm'


                # Define faixas ideais para sensores ambientais
                ideal_ranges = {
                    'temperature': (20, 30),   # °C
                    'humidity': (40, 60),      # %
                    'soil_humidity': (25, 45), # %
                    'wind_speed': (0, 10),     # km/h
                    'precipitation': (0, 5)    # mm
                }

                # Determina faixa ideal
                if sensor_name in ideal_ranges:
                    min_val, max_val = ideal_ranges[sensor_name]
                    ideal_range = f"{min_val}-{max_val}"
                else:
                    ideal_range = "N/A"

                # Determina impacto na colheita
                impact = "Desconhecido"
                if sensor_name in impact_info and isinstance(value, (int, float)):
                    for min_val, max_val, impact_text in impact_info[sensor_name]['ranges']:
                        if min_val <= value < max_val:
                            impact = impact_text
                            break

                print(f"{sensor_name[:20]:<20} | {str(value)[:10]:<10} | "
                    f"{unit:<7} | {ideal_range:<12} | {impact}")

        # Exibe sensores de emissão se existirem
        emission_sensors = [s for s in sensor_data.keys()
                        if 'emission' in s or s in ['ch4_emission', 'nh3_emission']]

        if emission_sensors:
            print("\n=== DADOS DE EMISSÃO ===")
            print("Sensor               | Valor      | Unidade | Faixa Ideal  | Status")
            print("-" * 75)

            # Definição de faixas de referência para emissões
            emission_ranges = {
                'ch4_emission': (0, 10), # kg/h
                'nh3_emission': (0, 8)   # kg/h
            }

            for sensor_name in sorted(emission_sensors):
                reading = sensor_data[sensor_name]

                # Extrai valor e unidade
                if isinstance(reading, dict) and 'value' in reading:
                    value = reading.get('value', 'N/A')
                    unit = reading.get('unit', '')
                else:
                    value = reading
                    unit = 'kg/h'

                # Determina faixa ideal e status
                if sensor_name in emission_ranges:
                    min_val, max_val = emission_ranges[sensor_name]
                    ideal_range = f"{min_val}-{max_val}"

                    if isinstance(value, (int, float)):
                        if value <= max_val:
                            status = "ACEITÁVEL"
                        else:
                            status = "ELEVADO"
                    else:
                        status = "N/A"
                else:
                    ideal_range = "N/A"
                    status = "N/A"

                print(f"{sensor_name[:20]:<20} | {str(value)[:10]:<10} | "
                    f"{unit:<7} | {ideal_range:<12} | {status}")


    def _view_critical_parameters(self, sensor_data_list, analysis_data_list):
        """
        Visualiza parâmetros críticos para perdas na colheita de cana-de-açúcar.

        Apresenta visão especializada que destaca os fatores mais importantes
        que afetam perdas na colheita mecanizada.

        Args:
            sensor_data_list (list): Lista de conjuntos de dados de sensores
            analysis_data_list (list): Lista de análises correspondentes
        """
        self._print_header("Visualização de Parâmetros Críticos para Perdas na Colheita")

        # Adiciona contexto para melhor compreensão
        print("\nEsta tela apresenta os principais parâmetros que afetam as perdas")
        print("na colheita mecanizada, suas faixas ideais e status atual.\n")

        # Adiciona legenda para status
        print("LEGENDA DE STATUS:")
        print("• IDEAL: Valor dentro da faixa recomendada")
        print("• ALTO: Valor acima do limite recomendado")
        print("• BAIXO: Valor abaixo do limite recomendado\n")

        if not sensor_data_list:
            print("\nNenhum dado disponível para análise!")
            self._wait_keypress()
            return

        # Define parâmetros críticos com faixas ideais
        critical_params = {
            'harvester_speed': {
                'name': 'Velocidade da Colheitadeira',
                'unit': 'km/h',
                'ideal_range': (4.5, 6.5),
                'impact': ('Velocidades fora da faixa ideal podem aumentar perdas. '
                        'Velocidade muito alta causa corte irregular e desprendimento '
                        'excessivo. Velocidade muito baixa reduz eficiência e pode '
                        'aumentar danos à soqueira.')
            },
            'cutting_height': {
                'name': 'Altura de Corte',
                'unit': 'mm',
                'ideal_range': (20, 30),
                'impact': ('Altura de corte adequada é crucial para minimizar perdas. '
                        'Corte muito alto deixa tocos com açúcar. Corte muito baixo '
                        'aumenta impurezas minerais e prejudica rebrota.')
            },
            'soil_humidity': {
                'name': 'Umidade do Solo',
                'unit': '%',
                'ideal_range': (25, 45),
                'impact': ('Umidade do solo afeta trafegabilidade e qualidade de corte. '
                        'Solo muito seco aumenta impurezas. Solo muito úmido causa '
                        'compactação e prejudica eficiência da colheita.')
            },
            'temperature': {
                'name': 'Temperatura Ambiente',
                'unit': '°C',
                'ideal_range': (20, 30),
                'impact': ('Temperatura afeta desempenho de máquinas e operadores. '
                        'Temperaturas muito altas aumentam perdas por dessecação '
                        'e reduzem eficiência de colheita.')
            },
            'wind_speed': {
                'name': 'Velocidade do Vento',
                'unit': 'km/h',
                'ideal_range': (0, 10),
                'impact': ('Vento forte prejudica o direcionamento preciso da colheita '
                        'e aumenta perdas por dispersão de material.')
            }
        }

        # Coleta últimos valores para parâmetros críticos
        latest_values = {}
        trend_data = {}

        # Inicializa estruturas de dados
        for param in critical_params:
            latest_values[param] = None
            trend_data[param] = []

        # Percorre dados de sensores (do mais recente para o mais antigo)
        for data_set in sensor_data_list:
            sensors = data_set.get('data', {})
            timestamp = data_set.get('timestamp')

            for param in critical_params:
                if param in sensors:
                    reading = sensors[param]

                    # Extrai valor numérico
                    if isinstance(reading, dict) and 'value' in reading:
                        value = reading.get('value')
                    else:
                        value = reading

                    # Armazena primeiro valor encontrado (mais recente)
                    if latest_values[param] is None and isinstance(value, (int, float)):
                        latest_values[param] = value

                    # Coleta dados para análise de tendência
                    if isinstance(value, (int, float)):
                        trend_data[param].append((timestamp, value))

        # Exibe resumo de parâmetros críticos
        print("\nRESUMO DE PARÂMETROS CRÍTICOS PARA COLHEITA DE CANA-DE-AÇÚCAR")
        print("=" * 75)
        print("Parâmetro            | Valor Atual | Faixa Ideal  | Status    | Tendência")
        print("-" * 75)

        # Para cada parâmetro crítico
        for param, details in critical_params.items():
            current_value = latest_values[param]
            if current_value is None:
                status = "N/A"
                trend = "N/A"
                current_str = "N/A"
            else:
                # Verifica status conforme faixa ideal
                min_val, max_val = details['ideal_range']
                ideal_range = f"{min_val}-{max_val}"

                if current_value < min_val:
                    status = "BAIXO"
                elif current_value > max_val:
                    status = "ALTO"
                else:
                    status = "IDEAL"

                # Formata valor atual
                current_str = f"{current_value:.1f}"

                # Calcula tendência se houver dados suficientes
                values = [v for _, v in trend_data[param]]
                if len(values) >= 3:
                    first_half = values[len(values)//2:]
                    second_half = values[:len(values)//2]

                    first_avg = sum(first_half) / len(first_half)
                    second_avg = sum(second_half) / len(second_half)

                    if second_avg > first_avg * 1.05:
                        trend = "↑ SUBINDO"
                    elif second_avg < first_avg * 0.95:
                        trend = "↓ DESCENDO"
                    else:
                        trend = "→ ESTÁVEL"
                else:
                    trend = "INSUFICIENTE"

            # Exibe linha do parâmetro
            print(f"{details['name'][:20]:<20} | {current_str:<11} | "
                f"{ideal_range:<12} | {status:<9} | {trend}")

        # Exibe detalhes de cada parâmetro
        print("\n\nDETALHES DOS PARÂMETROS CRÍTICOS")
        print("=" * 75)

        for param, details in critical_params.items():
            print(f"{details['name'].upper()} ({param})")
            print(f"\tFaixa ideal: {details['ideal_range'][0]}-{details['ideal_range'][1]} "
                f"{details['unit']}")
            print(f"\tImpacto na colheita: {details['impact']}\n")

            # Exibe análise de problemas relacionados a este parâmetro
            problem_count = 0
            for analysis in analysis_data_list:
                factors = analysis.get('analysis', {}).get('problematic_factors', [])
                for factor in factors:
                    if factor.get('factor') == param:
                        problem_count += 1

            if problem_count > 0:
                print(f"Observação: Este parâmetro foi identificado como problemático "
                    f"em {problem_count} medições nesta sessão.")

        # Exibe análise da perda total
        if analysis_data_list:
            print("\n\nANÁLISE DE PERDAS NA SESSÃO ATUAL")
            print("=" * 75)

            # Calcula estatísticas de perdas
            loss_values = []
            for analysis in analysis_data_list:
                loss = analysis.get('analysis', {}).get('loss_estimate')
                if isinstance(loss, (int, float)):
                    loss_values.append(loss)

            if loss_values:
                avg_loss = sum(loss_values) / len(loss_values)
                min_loss = min(loss_values)
                max_loss = max(loss_values)

                print(f"Perda média: {avg_loss:.2f}%")
                print(f"Perda mínima: {min_loss:.2f}%")
                print(f"Perda máxima: {max_loss:.2f}%")

                # Classifica perda média
                if avg_loss >= 15:
                    print("\nStatus: PERDA ALTA - Acima do limite aceitável!")
                    print("Recomendação: Revisar urgentemente parâmetros operacionais.")
                elif avg_loss >= 10:
                    print("\nStatus: PERDA MÉDIA - Acima do nível ideal.")
                    print("Recomendação: Ajustar parâmetros destacados como problemáticos.")
                elif avg_loss >= 5:
                    print("\nStatus: PERDA BAIXA - Dentro de limites aceitáveis.")
                    print("Recomendação: Monitorar parâmetros para manter ou reduzir perdas.")
                else:
                    print("\nStatus: PERDA MÍNIMA - Excelente desempenho.")
                    print("Recomendação: Manter condições atuais de operação.")

        self._wait_keypress()


    def _get_sensor_category(self, sensor_name):
        """
        Determina a categoria de um sensor baseado em seu nome.

        Args:
            sensor_name (str): Nome do sensor

        Returns:
            str: Categoria do sensor (operational, environmental, emission, other)
        """
        if sensor_name in ['harvester_speed', 'cutting_height']:
            return "operational"
        elif sensor_name in ['temperature', 'humidity', 'soil_humidity',
                            'wind_speed', 'precipitation', 'is_raining']:
            return "environmental"
        elif 'emission' in sensor_name or sensor_name in ['ch4_emission', 'nh3_emission']:
            return "emission"
        else:
            return "other"

    def _get_default_unit(self, sensor_name):
        """
        Retorna unidade padrão para um sensor.

        Args:
            sensor_name (str): Nome do sensor

        Returns:
            str: Unidade padrão do sensor
        """
        units = {
            'harvester_speed': 'km/h',
            'cutting_height': 'mm',
            'temperature': '°C',
            'humidity': '%',
            'soil_humidity': '%',
            'wind_speed': 'km/h',
            'precipitation': 'mm',
            'ch4_emission': 'kg/h',
            'nh3_emission': 'kg/h'
        }

        return units.get(sensor_name, '')

    def _view_sensor_history(self, sensor_data_list):
        """
        Visualiza histórico de um sensor específico ao longo do tempo.

        Args:
            sensor_data_list (list): Lista de conjuntos de dados de sensores
        """
        self._print_header("Visualização de Histórico de Sensor")

        # Adiciona explicação sobre a funcionalidade
        print("\nEsta tela permite acompanhar a evolução de um sensor específico")
        print("ao longo do tempo, facilitando a identificação de tendências e")
        print("comportamentos que podem afetar as perdas na colheita.\n")

        # Adiciona informação sobre como retornar
        print("Dica: Para retornar à tela anterior, digite 0.\n")

        # Identifica todos os sensores disponíveis
        all_sensors = set()
        for data in sensor_data_list:
            sensor_data = data.get('data', {})
            all_sensors.update(sensor_data.keys())

        if not all_sensors:
            print("\nNenhum sensor encontrado nos dados!")
            self._wait_keypress()
            return

        # Exibe lista de sensores disponíveis
        print("\nSensores disponíveis:")
        for i, sensor in enumerate(sorted(all_sensors), 1):
            print(f"{i}. {sensor}")

        # Solicita escolha do sensor
        choice = self._input_with_prompt(
            f"\nEscolha um sensor (1-{len(all_sensors)}) ou 0 para cancelar",
            "0"
        )

        # Verifica se a escolha é válida
        try:
            choice_idx = int(choice)
            if choice_idx == 0:
                return

            # Verifica se o índice está dentro do intervalo
            if 1 <= choice_idx <= len(all_sensors):
                selected_sensor = sorted(all_sensors)[choice_idx - 1]
                self._display_sensor_history(sensor_data_list, selected_sensor)
            else:
                print("\nNúmero de sensor inválido!")
        except ValueError:
            print("\nEntrada inválida! Digite um número.")

        self._wait_keypress()


    def _display_sensor_history(self, sensor_data_list, sensor_name):
        """
        Exibe histórico de um sensor específico com contexto agronômico.

        Args:
            sensor_data_list (list): Lista de conjuntos de dados de sensores
            sensor_name (str): Nome do sensor para exibir histórico
        """
        print(f"\nHistórico do sensor: {sensor_name}")
        print("-" * 75)

        # Determina a categoria do sensor para definir o formato de exibição
        category = self._get_sensor_category(sensor_name)

        # Define cabeçalho conforme categoria do sensor
        if category == "operational":
            print("Timestamp                  | Valor      | Unidade | Faixa Ideal  | Status")
            # Define faixas ideais para sensores operacionais
            ideal_ranges = {
                'harvester_speed': (4.5, 6.5),  # km/h
                'cutting_height': (20, 30),     # mm
            }
        elif category == "environmental":
            print("Timestamp                  | Valor      | Unidade | Faixa Ideal  | Impacto")
            # Define faixas ideais para sensores ambientais
            ideal_ranges = {
                'temperature': (20, 30),       # °C
                'humidity': (40, 60),          # %
                'soil_humidity': (25, 45),     # %
                'wind_speed': (0, 10),         # km/h
                'precipitation': (0, 5)        # mm
            }
        elif category == "emission":
            print("Timestamp                  | Valor      | Unidade | Faixa Ideal  | Status")
            # Define faixas ideais para sensores de emissão
            ideal_ranges = {
                'ch4_emission': (0, 10),        # kg/h
                'nh3_emission': (0, 8)          # kg/h
            }
        else:
            print("Timestamp                  | Valor      | Unidade")
            ideal_ranges = {}

        print("-" * 75)

        # Define mapeamentos de impacto/status conforme tipo de sensor
        impact_info = {
            'temperature': {
                'ranges': [(0, 15, "LIMITANTE"), (15, 20, "ADEQUADO"),
                        (20, 30, "IDEAL"), (30, 100, "CRÍTICO")]
            },
            'humidity': {
                'ranges': [(0, 40, "LIMITANTE"), (40, 60, "IDEAL"),
                        (60, 80, "ADEQUADO"), (80, 100, "CRÍTICO")]
            },
            'soil_humidity': {
                'ranges': [(0, 15, "CRÍTICO"), (15, 25, "LIMITANTE"),
                        (25, 45, "IDEAL"), (45, 60, "ADEQUADO"),
                        (60, 100, "CRÍTICO")]
            },
            'wind_speed': {
                'ranges': [(0, 5, "IDEAL"), (5, 10, "ADEQUADO"),
                        (10, 15, "LIMITANTE"), (15, 100, "CRÍTICO")]
            },
            'precipitation': {
                'ranges': [(0, 0.1, "IDEAL"), (0.1, 5, "ADEQUADO"),
                        (5, 10, "LIMITANTE"), (10, 20, "CRÍTICO"),
                        (20, 999, "IMPEDITIVO")]
            }
        }

        # Coleta histórico do sensor
        history = []
        for data in sensor_data_list:
            timestamp = data.get('timestamp', 'desconhecido')
            sensor_data = data.get('data', {})

            if sensor_name in sensor_data:
                reading = sensor_data[sensor_name]

                # Verifica formato da leitura (objeto ou valor simples)
                if isinstance(reading, dict) and 'value' in reading:
                    value = reading.get('value', 'N/A')
                    unit = reading.get('unit', '')
                    sensor_timestamp = reading.get('timestamp', timestamp)
                else:
                    value = reading
                    unit = self._get_default_unit(sensor_name)
                    sensor_timestamp = timestamp

                # Determina faixa ideal
                if sensor_name in ideal_ranges:
                    min_val, max_val = ideal_ranges[sensor_name]
                    ideal_range = f"{min_val}-{max_val}"

                    # Determina status ou impacto
                    if category == "operational":
                        if isinstance(value, (int, float)):
                            if value < min_val:
                                status = "BAIXO"
                            elif value > max_val:
                                status = "ALTO"
                            else:
                                status = "IDEAL"
                        else:
                            status = "N/A"
                    elif category == "environmental":
                        impact = "N/A"
                        if sensor_name in impact_info and isinstance(value, (int, float)):
                            for min_v, max_v, impact_text in impact_info[sensor_name]['ranges']:
                                if min_v <= value < max_v:
                                    impact = impact_text
                                    break
                        status = impact
                    elif category == "emission":
                        if isinstance(value, (int, float)):
                            if value <= max_val:
                                status = "ACEITÁVEL"
                            else:
                                status = "ELEVADO"
                        else:
                            status = "N/A"
                else:
                    ideal_range = "N/A"
                    status = "N/A"

                # Adiciona ao histórico com todas as informações
                history.append((sensor_timestamp, value, unit, ideal_range, status))

        # Ordena histórico por timestamp
        history.sort(reverse=True)

        # Exibe histórico com formato adequado para a categoria
        for item in history:
            timestamp, value, unit = item[0], item[1], item[2]

            if category in ["operational", "environmental", "emission"]:
                ideal_range, status = item[3], item[4]
                print(f"{timestamp} | {str(value)[:10]:<10} | {unit:<7} | "
                    f"{ideal_range:<12} | {status}")
            else:
                print(f"{timestamp} | {str(value)[:10]:<10} | {unit}")

    def _view_sensor_trends(self, sensor_data_list):
        """
        Visualiza tendências e estatísticas dos sensores.

        Args:
            sensor_data_list (list): Lista de conjuntos de dados de sensores
        """
        self._print_header("Visualização de Tendências e Estatísticas")

        # Adiciona explicação sobre a funcionalidade
        print("\nESTA TELA MOSTRA:")
        print("• Valores médios, mínimos e máximos de cada sensor")
        print("• Tendências de comportamento dos sensores")
        print("• Correlação com perdas na colheita\n")

        # Adiciona legenda para tendências
        print("LEGENDA DE TENDÊNCIAS:")
        print("• SUBINDO: Valores aumentando ao longo do tempo")
        print("• DESCENDO: Valores diminuindo ao longo do tempo")
        print("• ESTÁVEL: Valores mantendo-se dentro de limites estreitos\n")

        # Identifica todos os sensores disponíveis
        all_sensors = set()
        for data in sensor_data_list:
            sensor_data = data.get('data', {})
            all_sensors.update(sensor_data.keys())

        if not all_sensors:
            print("\nNenhum sensor encontrado nos dados!")
            self._wait_keypress()
            return

        # Calcula estatísticas para cada sensor
        stats = {}

        for sensor_name in all_sensors:
            values = []

            for data in sensor_data_list:
                sensor_data = data.get('data', {})

                if sensor_name in sensor_data:
                    reading = sensor_data[sensor_name]

                    # Extrai valor numérico
                    if isinstance(reading, dict) and 'value' in reading:
                        value = reading.get('value')
                    else:
                        value = reading

                    # Adiciona apenas valores numéricos
                    if isinstance(value, (int, float)):
                        values.append(value)

            # Calcula estatísticas se houver valores numéricos
            if values:
                stats[sensor_name] = {
                    'count': len(values),
                    'min': min(values),
                    'max': max(values),
                    'avg': sum(values) / len(values),
                    'first': values[-1] if values else None,  # Mais antigo na lista
                    'last': values[0] if values else None,    # Mais recente na lista
                    'trend': 'subindo' if len(values) > 1 and values[0] > values[-1] else
                            'descendo' if len(values) > 1 and values[0] < values[-1] else
                            'estável'
                }

        # Exibe estatísticas
        print("\nEstatísticas dos sensores:")
        print("-" * 70)
        print("Sensor               | Contagem | Mín      | Máx      | Média    | "
            "Tendência")
        print("-" * 70)

        for sensor_name in sorted(stats.keys()):
            stat = stats[sensor_name]
            print(f"{sensor_name[:20]:<20} | {stat['count']:<8} | "
                f"{stat['min']:<8.2f} | {stat['max']:<8.2f} | {stat['avg']:<8.2f} | "
                f"{stat['trend']}")

        self._wait_keypress()


    def _analyze_data(self):
        """
        Analisa dados de perdas e emissões na colheita de cana-de-açúcar.

        Permite ao usuário visualizar diferentes tipos de análises sobre os
        dados coletados, incluindo relatórios de perdas, emissões de GHG e
        tendências ao longo do tempo.
        """
        while True:
            self._print_header("Análise de Dados")

            if not self.session_id:
                print("\nNenhuma sessão ativa para análise!")
                self._wait_keypress()
                return

            # Verificar componentes necessários
            if not all([self.json_manager, self.data_analyzer]):
                print("\nErro: Componentes necessários não inicializados!")
                self._wait_keypress()
                return

            # Carrega dados para análise
            analysis_data = self._load_analysis_data()
            sensor_data = self._load_sensor_data()
            recommendation_data = self._load_recommendation_data()

            if not analysis_data and not sensor_data:
                print("\nNenhum dado disponível para análise!")
                self._wait_keypress()
                return

            # Apresenta menu de análise
            print("\nOPÇÕES DE ANÁLISE:")
            print("1. Análise de perdas na colheita")
            print("2. Análise de emissões de gases")
            print("3. Análise de tendências e previsões")
            print("4. Análise de fatores problemáticos")
            print("5. Gerar relatório consolidado")
            print("6. Voltar ao menu anterior")

            choice = self._input_with_prompt("\nEscolha uma opção", "6")

            if choice == '1':
                self._show_harvest_loss_analysis(analysis_data)
            elif choice == '2':
                self._show_emission_analysis(sensor_data)
            elif choice == '3':
                self._show_trend_analysis(analysis_data)
            elif choice == '4':
                self._show_factor_analysis(analysis_data, recommendation_data)
            elif choice == '5':
                self._show_consolidated_report(analysis_data, sensor_data,
                                            recommendation_data)
            elif choice == '6':
                return
            else:
                print("\nOpção inválida!")
                self._wait_keypress()

    def _load_analysis_data(self):
        """
        Carrega dados de análise para a sessão atual.

        Returns:
            list: Lista de registros de análise
        """
        analysis_data = []

        # Verifica se o diretório existe
        if not os.path.exists(self.json_manager.data_dirs['analysis']):
            return analysis_data

        # Lista arquivos de análise para esta sessão
        for filename in os.listdir(self.json_manager.data_dirs['analysis']):
            if filename.startswith(f"{self.session_id}-") and filename.endswith('.json'):
                file_path = os.path.join(self.json_manager.data_dirs['analysis'], filename)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if 'analysis' in data:
                            analysis_data.append(data)
                except json.JSONDecodeError:
                    continue

        return analysis_data

    def _load_sensor_data(self):
        """
        Carrega dados de sensores para a sessão atual.

        Returns:
            dict: Dicionário com dados de sensores por tipo
        """
        sensor_data = {
            'ch4_emission': [],
            'nh3_emission': [],
            'timestamps': []
        }

        # Verifica se o diretório existe
        if not os.path.exists(self.json_manager.data_dirs['sensor_data']):
            return sensor_data

        # Lista arquivos de sensores para esta sessão
        for filename in os.listdir(self.json_manager.data_dirs['sensor_data']):
            if filename.startswith(f"{self.session_id}-") and filename.endswith('.json'):
                file_path = os.path.join(
                    self.json_manager.data_dirs['sensor_data'], filename)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if 'data' in data:
                            sensor_values = data['data']

                            # Extrai dados de emissões
                            for emission_type in ['ch4_emission', 'nh3_emission']:
                                if emission_type in sensor_values:
                                    reading = sensor_values[emission_type]
                                    if isinstance(reading, dict) and 'value' in reading:
                                        sensor_data[emission_type].append(reading['value'])
                                    else:
                                        sensor_data[emission_type].append(reading)

                            # Armazena timestamp
                            if any(emission_type in sensor_values
                                for emission_type in ['ch4_emission', 'nh3_emission']):
                                sensor_data['timestamps'].append(data['timestamp'])
                except json.JSONDecodeError:
                    continue

        return sensor_data

    def _load_recommendation_data(self):
        """
        Carrega dados de recomendações para a sessão atual.

        Returns:
            list: Lista de recomendações
        """
        recommendations = []

        # Verifica se o diretório existe
        if not os.path.exists(self.json_manager.data_dirs['recommendations']):
            return recommendations

        # Lista arquivos de recomendações para esta sessão
        for filename in os.listdir(self.json_manager.data_dirs['recommendations']):
            if filename.startswith(f"{self.session_id}-") and filename.endswith('.json'):
                file_path = os.path.join(
                    self.json_manager.data_dirs['recommendations'], filename)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if ('recommendations' in data and
                            'recommendations' in data['recommendations']):
                            for rec in data['recommendations']['recommendations']:
                                # Evita duplicados
                                if rec not in recommendations:
                                    recommendations.append(rec)
                except json.JSONDecodeError:
                    continue

        return recommendations


    def _show_harvest_loss_analysis(self, analysis_data):
        """
        Exibe análise de perdas na colheita com formatação UI/UX adequada.

        Args:
            analysis_data (list): Lista de registros de análise
        """
        self._print_header("Análise de Perdas na Colheita")

        if not analysis_data:
            print("\nDados insuficientes para análise!")
            self._wait_keypress()
            return


        # Adiciona explicação contextual sobre a tela
        print("\nEsta tela apresenta análises detalhadas sobre as perdas na colheita,")
        print("incluindo estatísticas, distribuição por categorias e impacto econômico.")
        print("As informações ajudam a identificar o nível atual de perdas e orientar")
        print("decisões para otimização do processo de colheita.")


        # Delega análise para o componente especializado
        results = self.data_analyzer.analyze_harvest_losses(analysis_data)

        if "error" in results:
            print(f"\n{results['error']}")
            self._wait_keypress()
            return

        # Exibe informações gerais
        print(f"\nDados analisados: {results['count']} registros")

        # Seção de estatísticas - formatação amigável, não tabular
        self._print_section("ESTATÍSTICAS DE PERDAS")

        # Determina indicadores visuais com base na categoria
        loss_category = results['loss_category'].upper()
        if loss_category == "HIGH":
            category_indicator = "! CRÍTICO"
        elif loss_category == "MEDIUM":
            category_indicator = "▲ ALTO"
        elif loss_category == "LOW":
            category_indicator = "△ MODERADO"
        else:
            category_indicator = "✓ ACEITÁVEL"

        print(f"Perda média.......: {results['avg_loss']:.2f}% ({category_indicator})")
        print(f"Perda mínima......: {results['min_loss']:.2f}%")
        print(f"Perda máxima......: {results['max_loss']:.2f}%")

        # Indicador visual para tendência
        trend = results['trend'].upper()
        if trend == "AUMENTANDO":
            trend_indicator = "↑ AUMENTANDO"
        elif trend == "DIMINUINDO":
            trend_indicator = "↓ DIMINUINDO"
        else:
            trend_indicator = "→ ESTÁVEL"

        print(f"Tendência.........: {trend_indicator}")

        # Seção de distribuição - representação visual melhorada e alinhada
        self._print_section("DISTRIBUIÇÃO POR CATEGORIA")

        total = results['count']
        categories = results['categories']

        # Tamanho máximo da barra (caracteres)
        max_bar_size = 20

        # Cria representação visual com delimitadores claros e alinhamento corrigido
        high_pct = categories['high']/total*100
        med_pct = categories['medium']/total*100
        low_pct = categories['low']/total*100
        min_pct = categories['minimal']/total*100

        # Função para criar barra de progresso com delimitadores
        def create_bar(percentage):
            bar_size = int(percentage/100 * max_bar_size)
            return f"[{'█' * bar_size}{' ' * (max_bar_size - bar_size)}]"

        # Usando formatação com largura fixa para percentuais para garantir alinhamento
        print(f"ALTA (≥15%).......:   {categories['high']:1d} ({high_pct:6.1f}%) "
            f"{create_bar(high_pct)}")
        print(f"MÉDIA (10-15%)....:   {categories['medium']:1d} ({med_pct:6.1f}%) "
            f"{create_bar(med_pct)}")
        print(f"BAIXA (5-10%).....:   {categories['low']:1d} ({low_pct:6.1f}%) "
            f"{create_bar(low_pct)}")
        print(f"MÍNIMA (<5%)......:   {categories['minimal']:1d} ({min_pct:6.1f}%) "
            f"{create_bar(min_pct)}")

        # Adiciona legenda para interpretação
        print("\nLEGENDA:")
        print("█ = 5% das ocorrências  |  [ ] = escala de 0% a 100%")

        # Seção de impacto econômico
        self._print_section("IMPACTO ECONÔMICO ESTIMADO")

        economic = results['economic_impact']
        # Cálculo da perda percentual
        loss_pct = ((economic['potential_yield'] - economic['actual_yield']) /
                economic['potential_yield'] * 100)

        print(f"Produtividade potencial..: {economic['potential_yield']:.1f} ton/ha")
        print(f"Produtividade com perdas.: {economic['actual_yield']:.1f} ton/ha")
        print(f"Perda de produtividade...: {loss_pct:.1f}%")
        print(f"Impacto financeiro.......: R$ {economic['loss_value']:.2f}/ha")

        # Adiciona recomendação baseada na análise
        self._print_section("RECOMENDAÇÃO")

        if results['avg_loss'] >= 15:
            print("! AÇÃO URGENTE NECESSÁRIA")
            print("  As perdas na colheita estão acima do limite aceitável.")
            print("  Recomenda-se revisão imediata dos parâmetros operacionais.")
        elif results['avg_loss'] >= 10:
            print("! ATENÇÃO")
            print("  As perdas na colheita estão elevadas.")
            print("  Ajuste os parâmetros conforme recomendações específicas.")
        elif results['avg_loss'] >= 5:
            print("△ MONITORAMENTO")
            print("  As perdas na colheita estão em nível moderado.")
            print("  Mantenha monitoramento contínuo dos fatores críticos.")
        else:
            print("✓ DESEMPENHO ADEQUADO")
            print("  As perdas na colheita estão em nível aceitável.")
            print("  Continue com as práticas atuais.")

        print("\nLEGENDA DE STATUS:")
        print("! CRÍTICO  | ▲ ALTO  | △ MODERADO  | ✓ ACEITÁVEL")

        self._wait_keypress()

    def _show_emission_analysis(self, sensor_data):
        """
        Exibe análise de emissões de gases com formatação UI/UX adequada.

        Args:
            sensor_data (dict): Dicionário com dados de sensores
        """
        self._print_header("Análise de Emissões de Gases")

        if not sensor_data['timestamps']:
            print("\nNenhum dado de emissão encontrado!")
            self._wait_keypress()
            return

        # Adiciona explicação contextual sobre a tela
        print("\nEsta tela apresenta análise detalhada das emissões de gases durante")
        print("o processo de colheita, com foco em CH₄ (metano) e NH₃ (amônia).")
        print("As informações incluem níveis médios, mínimos e máximos, comparação")
        print("com valores de referência e impacto ambiental estimado.")
        print("\nDICA: Valores elevados de emissões podem indicar ineficiências no")
        print("processo ou ajustes necessários nos parâmetros operacionais.")

        # Delega análise para o componente especializado
        results = self.data_analyzer.analyze_emissions(sensor_data)

        print(f"\nDados analisados: {len(sensor_data['timestamps'])} registros")
        print(f"Período: {sensor_data['timestamps'][0][:16]} a "
            f"{sensor_data['timestamps'][-1][:16]}")

        # Seção de emissões de metano
        if 'ch4' in results:
            self._print_section("EMISSÕES DE METANO (CH₄)")

            ch4 = results['ch4']

            # Status visual
            if ch4['status'] == "alta":
                status_indicator = "! ELEVADO"
            elif ch4['status'] == "média":
                status_indicator = "▲ MODERADO"
            else:
                status_indicator = "✓ ACEITÁVEL"

            print(f"Emissão média.....: {ch4['avg']:.2f} kg/h ({status_indicator})")
            print(f"Emissão mínima....: {ch4['min']:.2f} kg/h")
            print(f"Emissão máxima....: {ch4['max']:.2f} kg/h")
            print(f"Equivalente em CO₂.: {ch4['co2e']:.2f} kg CO₂e/h")

            # Visualização com escala
            reference = 10.0  # Valor de referência para escala
            percentage = min(100, ch4['avg'] / reference * 100)

            bar_size = 20
            filled = int(percentage / 100 * bar_size)

            print("\nEscala de emissão:")
            print(f"0{'-'*9}|{'-'*10}10 kg/h")
            print(f"[{'█' * filled}{' ' * (bar_size - filled)}] {ch4['avg']:.2f} kg/h")

        # Seção de emissões de amônia
        if 'nh3' in results:
            self._print_section("EMISSÕES DE AMÔNIA (NH₃)")

            nh3 = results['nh3']

            # Status visual
            if nh3['status'] == "alta":
                status_indicator = "! ELEVADO"
            elif nh3['status'] == "média":
                status_indicator = "▲ MODERADO"
            else:
                status_indicator = "✓ ACEITÁVEL"

            print(f"Emissão média.....: {nh3['avg']:.2f} kg/h ({status_indicator})")
            print(f"Emissão mínima....: {nh3['min']:.2f} kg/h")
            print(f"Emissão máxima....: {nh3['max']:.2f} kg/h")

            # Visualização com escala
            reference = 8.0  # Valor de referência para escala
            percentage = min(100, nh3['avg'] / reference * 100)

            bar_size = 20
            filled = int(percentage / 100 * bar_size)

            print("\nEscala de emissão:")
            print(f"0{'-'*9}|{'-'*9}8 kg/h")
            print(f"[{'█' * filled}{' ' * (bar_size - filled)}] {nh3['avg']:.2f} kg/h")

        # Seção de impacto ambiental
        self._print_section("IMPACTO AMBIENTAL")
        print("• As emissões de metano (CH₄) contribuem diretamente para o efeito")
        print("  estufa, com potencial de aquecimento global 28 vezes maior que o CO₂.")
        print("• As emissões de amônia (NH₃) causam impactos indiretos como")
        print("  acidificação do solo e eutrofização de corpos d'água.")

        # Seção de recomendações
        self._print_section("RECOMENDAÇÕES PARA REDUÇÃO DE EMISSÕES")

        # Determina quais recomendações exibir com base nos resultados
        recommendations = []

        if 'ch4' in results and results['ch4']['status'] != "aceitável":
            recommendations.append("• Ajustar parâmetros operacionais da colheitadeira")
            recommendations.append("• Verificar regulagem dos sistemas de combustão")

        if 'nh3' in results and results['nh3']['status'] != "aceitável":
            recommendations.append("• Revisar práticas de manejo do solo")
            recommendations.append("• Otimizar aplicação de fertilizantes nitrogenados")

        if not recommendations:
            recommendations.append("• Manter parâmetros atuais de operação")
            recommendations.append("• Continuar com monitoramento regular")

        for rec in recommendations:
            print(rec)

        print("\nLEGENDA DE STATUS:")
        print("! ELEVADO  | ▲ MODERADO  | ✓ ACEITÁVEL")

        self._wait_keypress()


    def _show_trend_analysis(self, analysis_data):
        """
        Exibe análise de tendências com formatação UI/UX adequada.

        Args:
            analysis_data (list): Lista de registros de análise
        """
        self._print_header("Análise de Tendências e Previsões")

        if not analysis_data or len(analysis_data) < 3:
            print("\nDados insuficientes para análise de tendências!")
            print("São necessários pelo menos 3 registros para identificar padrões.")
            self._wait_keypress()
            return

        # Adiciona explicação contextual sobre a tela
        print("\nEsta tela mostra a evolução das perdas ao longo do tempo,")
        print("identificando tendências, padrões sazonais e projeções futuras.")
        print("A visualização temporal ajuda a avaliar a eficácia de ajustes")
        print("realizados e prever comportamentos futuros com base em dados históricos.")

        print("\nLEGENDA DE TENDÊNCIAS:")
        print("• ↑ AUMENTANDO: Valores crescentes ao longo do tempo")
        print("• ↓ DIMINUINDO: Valores decrescentes ao longo do tempo")
        print("• → ESTÁVEL: Valores sem variação significativa")

        # Delega análise para o componente especializado
        results = self.data_analyzer.analyze_trends(analysis_data)

        if "error" in results:
            print(f"\n{results['error']}")
            self._wait_keypress()
            return

        print(f"\nDados analisados: {len(analysis_data)} registros")
        if 'timestamps' in results and results['timestamps']:
            print(f"Período: {results['timestamps'][0][:16]} a "
                f"{results['timestamps'][-1][:16]}")

        # Seção de tendência de perdas
        self._print_section("TENDÊNCIA DE PERDAS NA COLHEITA")

        # Exibe gráfico de tendência simplificado
        if 'loss_values' in results and results['loss_values']:
            samples = min(10, len(results['loss_values']))
            values = results['loss_values'][-samples:]

            # Determina escala do gráfico
            max_val = max(values)
            min_val = min(values)
            range_val = max(1, max_val - min_val)

            # Altura do gráfico
            height = 5

            print("Evolução recente das perdas:")
            print(f"    {max_val:.1f}% ┬")

            # Desenha o gráfico
            for h in range(height, 0, -1):
                threshold = min_val + (range_val / height) * h
                line = "         │"
                for val in values:
                    if val >= threshold:
                        line += "█"
                    else:
                        line += " "
                print(line)

            bottom_line = f"    {min_val:.1f}% ┴" + "─" * samples
            print(bottom_line)

            # Exibe direção da tendência com indicador visual
            trend_direction = results.get('trend_direction', 'stable')
            if trend_direction == "increasing":
                trend_text = "↑ AUMENTANDO"
            elif trend_direction == "decreasing":
                trend_text = "↓ DIMINUINDO"
            else:
                trend_text = "→ ESTÁVEL"

            print(f"\nTendência geral: {trend_text}")

            # Verifica se há previsão disponível e se não é None
            if 'prediction' in results and results['prediction'] is not None:
                prediction = results['prediction']
                print(f"Previsão para próximos ciclos: {prediction:.2f}%")

                if prediction > values[-1]:
                    print("! ALERTA: Tendência de aumento nas perdas")
                else:
                    print("✓ Tendência de redução nas perdas")

        # Seção de fatores recorrentes
        if 'common_factors' in results and results['common_factors']:
            self._print_section("FATORES PROBLEMÁTICOS RECORRENTES")

            for i, factor in enumerate(results['common_factors']):
                print(f"#{i+1}: {factor['factor'].upper()}")
                print(f"    • Frequência: {factor['frequency']*100:.1f}% das análises")
                print(f"    • Severidade média: {factor['severity']:.2f}")

                # Determina direção predominante
                direction = factor.get('direction', '')
                if direction == "above":
                    dir_text = "acima do ideal"
                elif direction == "below":
                    dir_text = "abaixo do ideal"
                else:
                    dir_text = "variável"

                print(f"    • Direção: {dir_text}")

        # Seção de recomendações baseadas em tendências
        self._print_section("AÇÕES RECOMENDADAS")

        trend_direction = results.get('trend_direction', 'stable')
        if trend_direction == "increasing":
            print("! ATENÇÃO - TENDÊNCIA DE AUMENTO NAS PERDAS")
            print("• Revisar urgentemente os parâmetros operacionais")
            print("• Focar nos fatores problemáticos recorrentes identificados")
            print("• Implementar monitoramento mais frequente")
        elif trend_direction == "decreasing":
            print("✓ TENDÊNCIA POSITIVA - REDUÇÃO NAS PERDAS")
            print("• Manter os ajustes recentes nos parâmetros")
            print("• Documentar as práticas bem-sucedidas")
            print("• Continuar com o monitoramento regular")
        else:
            print("→ TENDÊNCIA ESTÁVEL")
            print("• Manter vigilância sobre fatores críticos identificados")
            print("• Avaliar oportunidades para otimização adicional")

        print("\nLEGENDA DE TENDÊNCIAS:")
        print("↑ AUMENTANDO | ↓ DIMINUINDO | → ESTÁVEL")

        self._wait_keypress()

    def _show_factor_analysis(self, analysis_data, recommendation_data):
        """
        Exibe análise de fatores problemáticos com formatação UI/UX adequada.

        Args:
            analysis_data (list): Lista de registros de análise
            recommendation_data (list): Lista de recomendações
        """
        self._print_header("Análise de Fatores Problemáticos")

        if not analysis_data:
            print("\nDados insuficientes para análise de fatores!")
            self._wait_keypress()
            return

        # Adiciona explicação contextual sobre a tela
        print("\nEsta tela identifica e classifica os fatores que mais contribuem")
        print("para as perdas na colheita de cana-de-açúcar, organizados por")
        print("frequência, severidade e impacto. Para cada fator crítico, são")
        print("apresentadas recomendações específicas para mitigação.")

        print("\nLEGENDA DE SÍMBOLOS:")
        print("• ! Alta prioridade | △ Média prioridade | ○ Baixa prioridade")
        print("• ↑ ACIMA do ideal | ↓ ABAIXO do ideal | ↕ VARIÁVEL")

        # Delega análise para o componente especializado
        results = self.data_analyzer.analyze_factors(analysis_data)

        if "error" in results:
            print(f"\n{results['error']}")
            self._wait_keypress()
            return

        print(f"\nDados analisados: {results['total_analyses']} registros")
        print(f"Fatores problemáticos identificados: {results['factors_count']}")

        # Ordenar fatores por impacto
        sorted_factors = results['sorted_factors']

        # Seção de fatores críticos
        self._print_section("FATORES CRÍTICOS IDENTIFICADOS")

        if not sorted_factors:
            print("Nenhum fator problemático identificado nas análises.")
        else:
            # Exibe detalhes dos fatores mais críticos (top 3 ou menos)
            for i, factor_name in enumerate(sorted_factors[:3]):
                if i > 0:
                    print("")  # Linha em branco entre fatores

                factor = results['factors'][factor_name]

                # Formatar nome do fator
                print(f"#{i+1}: {factor_name.upper()}")

                # Dados estatísticos com verificação de existência das chaves
                frequency = factor.get('frequency', 0) * 100
                print(f"• Ocorrências......: {factor.get('count', 0)} ({frequency:.1f}% das análises)")
                print(f"• Severidade média.: {factor.get('avg_severity', 0):.2f}")
                print(f"• Valor médio......: {factor.get('avg_value', 0):.2f}")

                # Determina direção predominante com indicador visual
                direction = factor['direction']
                if direction == "above":
                    dir_text = "↑ ACIMA do ideal"
                elif direction == "below":
                    dir_text = "↓ ABAIXO do ideal"
                else:
                    dir_text = "↕ VARIÁVEL"

                print(f"• Direção..........: {dir_text}")

                # Exibe faixa ideal se disponível
                if factor['optimal_range']:
                    opt_min, opt_max = factor['optimal_range']
                    print(f"• Faixa ideal......: {opt_min} - {opt_max}")

                # Visualização de impacto
                impact = factor['impact'] * 100
                impact_bar = int(impact / 5)  # Escala de 0-20 caracteres

                print(f"• Impacto..........: {impact:.1f}%")
                print(f"  [{'█' * impact_bar}{' ' * (20 - impact_bar)}]")

        # Seção de recomendações associadas
        if recommendation_data:
            self._print_section("RECOMENDAÇÕES RELACIONADAS")

            # Filtra recomendações relevantes para os fatores críticos
            relevant_recs = []
            for rec in recommendation_data:
                if 'factor' in rec and rec['factor'] in sorted_factors[:3]:
                    relevant_recs.append(rec)

            if not relevant_recs:
                print("Nenhuma recomendação específica encontrada para os fatores críticos.")
            else:
                # Agrupa recomendações por fator
                recs_by_factor = {}
                for rec in relevant_recs:
                    factor = rec['factor']
                    if factor not in recs_by_factor:
                        recs_by_factor[factor] = []
                    recs_by_factor[factor].append(rec)

                # Exibe recomendações para cada fator
                for factor in sorted_factors[:3]:
                    if factor in recs_by_factor:
                        print(f"{factor.upper()}:")
                        for rec in recs_by_factor[factor][:2]:  # Top 2 recomendações
                            priority = rec.get('priority', '').upper()
                            if priority == "HIGH":
                                priority_mark = "!"
                            elif priority == "MEDIUM":
                                priority_mark = "△"
                            else:
                                priority_mark = "○"

                            print(f"• [{priority_mark}] {rec.get('text', '')}")

        # Seção de ações sugeridas
        self._print_section("AÇÕES SUGERIDAS")

        if sorted_factors:
            top_factor = sorted_factors[0]
            factor_data = results['factors'][top_factor]

            print(f"FOCO PRINCIPAL: {top_factor.upper()}")

            if factor_data['direction'] == "above":
                print(f"• Reduzir {top_factor} para a faixa ideal de "
                    f"{factor_data['optimal_range'][0]}-{factor_data['optimal_range'][1]}")
            elif factor_data['direction'] == "below":
                print(f"• Aumentar {top_factor} para a faixa ideal de "
                    f"{factor_data['optimal_range'][0]}-{factor_data['optimal_range'][1]}")

            print("• Implementar monitoramento contínuo deste parâmetro")
            print("• Priorizar as recomendações específicas para este fator")
        else:
            print("• Manter os parâmetros atuais de operação")
            print("• Continuar com monitoramento regular")

        print("\nLEGENDA DE DIREÇÃO:")
        print("↑ ACIMA do ideal | ↓ ABAIXO do ideal | ↕ VARIÁVEL")
        print("\nLEGENDA DE PRIORIDADE:")
        print("! Alta | △ Média | ○ Baixa")

        self._wait_keypress()

    def _show_consolidated_report(self, analysis_data, sensor_data, recommendation_data):
        """
        Exibe relatório consolidado com formatação UI/UX adequada.

        Args:
            analysis_data (list): Lista de registros de análise
            sensor_data (dict): Dicionário com dados de sensores
            recommendation_data (list): Lista de recomendações
        """
        self._print_header("Relatório Consolidado")

        if not analysis_data and not sensor_data:
            print("\nDados insuficientes para gerar relatório!")
            self._wait_keypress()
            return

        # Adiciona explicação contextual sobre a tela
        print("\nEste relatório integra os resultados de todas as análises,")
        print("fornecendo uma visão completa sobre perdas, emissões, fatores")
        print("críticos e recomendações prioritárias. O documento consolida as")
        print("principais informações em um formato conciso para facilitar a")
        print("tomada de decisões e planejamento de ações corretivas.")

        print("\nO relatório está organizado em cinco seções principais:")
        print("1. Resumo de perdas | 2. Emissões | 3. Fatores críticos")
        print("4. Recomendações    | 5. Conclusões e próximos passos")

        # Processa análises necessárias
        loss_analysis = {}
        emission_analysis = {}
        factor_analysis = {}

        if analysis_data:
            loss_analysis = self.data_analyzer.analyze_harvest_losses(analysis_data)
            factor_analysis = self.data_analyzer.analyze_factors(analysis_data)

        if sensor_data and sensor_data['timestamps']:
            emission_analysis = self.data_analyzer.analyze_emissions(sensor_data)

        # Delega geração do relatório consolidado
        report = self.data_analyzer.generate_consolidated_report(
            loss_analysis, emission_analysis, factor_analysis, recommendation_data
        )

        # Título e informações gerais
        print("\n" + "=" * 78)
        print(" RELATÓRIO CONSOLIDADO: PERDAS E EMISSÕES NA COLHEITA DE CANA-DE-AÇÚCAR ")
        print("=" * 78)

        # Informações da sessão
        print(f"\nSESSÃO: {self.session_id}")
        print(f"DATA: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if analysis_data:
            print(f"PERÍODO ANALISADO: {analysis_data[0]['timestamp'][:16]} a "
                f"{analysis_data[-1]['timestamp'][:16]}")

        total_records = len(analysis_data) if analysis_data else 0
        print(f"REGISTROS ANALISADOS: {total_records}")

        # Resumo de perdas
        if loss_analysis and 'avg_loss' in loss_analysis:
            self._print_section("1. RESUMO DE PERDAS NA COLHEITA")

            avg_loss = loss_analysis['avg_loss']
            category = loss_analysis['loss_category'].upper()

            # Indicador visual de status
            if category == "HIGH":
                status_indicator = "! CRÍTICO"
            elif category == "MEDIUM":
                status_indicator = "▲ ALTO"
            elif category == "LOW":
                status_indicator = "△ MODERADO"
            else:
                status_indicator = "✓ ACEITÁVEL"

            print(f"• Perda média.......: {avg_loss:.2f}% ({status_indicator})")

            # Visualização com barra
            print(f"  [{'█' * int(avg_loss)}{'·' * (20 - int(avg_loss))}] "
                f"(escala: 0-20%)")

            # Impacto econômico
            if 'economic_impact' in loss_analysis:
                economic = loss_analysis['economic_impact']
                print(f"• Impacto econômico.: R$ {economic['loss_value']:.2f}/ha")

        # Resumo de emissões
        if emission_analysis:
            self._print_section("2. RESUMO DE EMISSÕES DE GASES")

            if 'ch4' in emission_analysis:
                ch4 = emission_analysis['ch4']
                ch4_status = ch4['status'].upper()

                if ch4_status == "ALTA":
                    ch4_indicator = "! ELEVADO"
                elif ch4_status == "MÉDIA":
                    ch4_indicator = "▲ MODERADO"
                else:
                    ch4_indicator = "✓ ACEITÁVEL"

                print(f"• Emissão CH₄......: {ch4['avg']:.2f} kg/h ({ch4_indicator})")
                print(f"• Equivalente CO₂e.: {ch4['co2e']:.2f} kg CO₂e/h")

            if 'nh3' in emission_analysis:
                nh3 = emission_analysis['nh3']
                nh3_status = nh3['status'].upper()

                if nh3_status == "ALTA":
                    nh3_indicator = "! ELEVADO"
                elif nh3_status == "MÉDIA":
                    nh3_indicator = "▲ MODERADO"
                else:
                    nh3_indicator = "✓ ACEITÁVEL"

                print(f"• Emissão NH₃......: {nh3['avg']:.2f} kg/h ({nh3_indicator})")

        # Fatores críticos identificados
        if 'top_factors' in report and report['top_factors']:
            self._print_section("3. FATORES CRÍTICOS IDENTIFICADOS")

            for i, factor in enumerate(report['top_factors']):
                # Indicador visual de direção
                if factor['direction'] == "above":
                    dir_text = "↑ ACIMA"
                elif factor['direction'] == "below":
                    dir_text = "↓ ABAIXO"
                else:
                    dir_text = "↕ VARIÁVEL"

                impact = factor['impact'] * 100

                print(f"{i+1}. {factor['name'].upper()}")
                print(f"   • Frequência: {factor['frequency']*100:.1f}% das análises")
                print(f"   • Direção: {dir_text} do ideal")
                print(f"   • Impacto: {impact:.1f}%")

        # Recomendações principais
        if 'priority_recommendations' in report and report['priority_recommendations']:
            self._print_section("4. RECOMENDAÇÕES PRIORITÁRIAS")

            for i, rec in enumerate(report['priority_recommendations']):
                priority = rec.get('priority', '').upper()
                if priority == "HIGH":
                    priority_mark = "!"
                elif priority == "MEDIUM":
                    priority_mark = "△"
                else:
                    priority_mark = "○"

                factor = rec.get('factor', 'geral')
                text = rec.get('text', '')

                print(f"{i+1}. [{priority_mark} ] {text}")
                if factor != 'general':
                    print(f"   Fator relacionado: {factor}")

        # Conclusão e próximos passos
        if 'conclusion' in report:
            self._print_section("5. CONCLUSÃO E PRÓXIMOS PASSOS")

            print(report['conclusion'])
            print("")

            if 'next_steps' in report:
                for i, step in enumerate(report['next_steps']):
                    print(f"{i+1}. {step}")

        # Legenda
        print("\nLEGENDA DE INDICADORES:")
        print("! CRÍTICO/ELEVADO | ▲ ALTO/MODERADO    | △ MÉDIO    | ✓ ACEITÁVEL")
        print("↑ ACIMA do ideal  | ↓ ABAIXO do ideal  | ↕ VARIÁVEL | → ESTÁVEL")
        print("! Alta prioridade | △ Média prioridade | ○ Baixa prioridade")

        self._wait_keypress()


    def _view_recommendations(self):
        """
        Visualiza recomendações geradas para redução de perdas na colheita.

        Apresenta recomendações organizadas por prioridade e fator relacionado,
        com opções para filtrar por tipo e exibir detalhes específicos.
        """
        while True:
            self._print_header("Recomendações")

            if not self.session_id:
                print("\nNenhuma sessão ativa para visualizar recomendações!")
                self._wait_keypress()
                return

            # Carrega dados necessários
            recommendation_data = self._load_recommendation_data()
            analysis_data = self._load_analysis_data()

            if not recommendation_data:
                print("\nNenhuma recomendação encontrada para esta sessão!")
                self._wait_keypress()
                return

            # Adiciona explicação contextual sobre a tela
            print("\nEsta tela apresenta recomendações para reduzir perdas na colheita,")
            print("organizadas por prioridade e fator relacionado. As recomendações")
            print("são baseadas na análise dos dados coletados pelos sensores durante")
            print("a simulação de colheita.")

            # Exibe resumo de recomendações
            total_recs = len(recommendation_data)
            high_priority = sum(1 for r in recommendation_data
                            if r.get('priority') == 'high')
            medium_priority = sum(1 for r in recommendation_data
                                if r.get('priority') == 'medium')
            low_priority = sum(1 for r in recommendation_data
                            if r.get('priority') == 'low')

            print(f"\nTotal de recomendações: {total_recs}")
            print(f"• Alta prioridade: {high_priority}")
            print(f"• Média prioridade: {medium_priority}")
            print(f"• Baixa prioridade: {low_priority}")

            # Menu de visualização
            print("\nOPÇÕES DE VISUALIZAÇÃO:")
            print("1. Ver recomendações por prioridade")
            print("2. Ver recomendações por fator")
            print("3. Ver recomendações gerais")
            print("4. Ver todas as recomendações")
            print("5. Filtrar recomendações")
            print("6. Exportar recomendações")
            print("7. Voltar ao menu anterior")

            choice = self._input_with_prompt("\nEscolha uma opção", "7")

            if choice == '1':
                self._view_recommendations_by_priority(recommendation_data)
            elif choice == '2':
                # Agrupa recomendações por fator
                factors = {}
                for rec in recommendation_data:
                    factor = rec.get('factor', 'general')
                    if factor != 'general':
                        if factor not in factors:
                            factors[factor] = []
                        factors[factor].append(rec)

                self._view_recommendations_by_factor(factors)
            elif choice == '3':
                # Identifica recomendações gerais
                general_recs = [r for r in recommendation_data
                            if r.get('factor', 'general') == 'general']
                self._view_general_recommendations(general_recs)
            elif choice == '4':
                self._view_all_recommendations(recommendation_data)
            elif choice == '5':
                filtered_recs = self._filter_recommendations(recommendation_data)
                if filtered_recs:
                    self._view_all_recommendations(filtered_recs)
            elif choice == '6':
                self._export_recommendations(recommendation_data)
            elif choice == '7':
                return
            else:
                print("Opção inválida!")
                self._wait_keypress()


    def _view_recommendations_by_priority(self, recommendations):
        """
        Exibe recomendações organizadas por nível de prioridade.

        Args:
            recommendations (list): Lista de recomendações
        """
        self._print_header("Recomendações por Prioridade")

        # Adiciona explicação contextual
        print("\nEsta tela mostra as recomendações agrupadas por nível de prioridade.")
        print("Recomendações de alta prioridade devem ser implementadas primeiro")
        print("para obter o maior impacto na redução de perdas.")

        # Agrupa por prioridade
        by_priority = {
            'high': [],
            'medium': [],
            'low': []
        }

        for rec in recommendations:
            priority = rec.get('priority', 'low')
            by_priority[priority].append(rec)

        # Exibe recomendações de alta prioridade
        if by_priority['high']:
            self._print_section("ALTA PRIORIDADE (!)")
            for i, rec in enumerate(by_priority['high'], 1):
                factor = rec.get('factor', 'geral')
                text = rec.get('text', '')
                print(f"{i}. {text}")
                print(f"   Fator relacionado: {factor}")

        # Exibe recomendações de média prioridade
        if by_priority['medium']:
            self._print_section("MÉDIA PRIORIDADE (△)")
            for i, rec in enumerate(by_priority['medium'], 1):
                factor = rec.get('factor', 'geral')
                text = rec.get('text', '')
                print(f"{i}. {text}")
                print(f"   Fator relacionado: {factor}")

        # Exibe recomendações de baixa prioridade
        if by_priority['low']:
            self._print_section("BAIXA PRIORIDADE (○)")
            for i, rec in enumerate(by_priority['low'], 1):
                factor = rec.get('factor', 'geral')
                text = rec.get('text', '')
                print(f"{i}. {text}")
                print(f"   Fator relacionado: {factor}")

        # Adiciona legenda
        print("\nLEGENDA DE PRIORIDADE:")
        print("! Alta | △ Média | ○ Baixa")

        self._wait_keypress()


    def _view_recommendations_by_factor(self, factors):
        """
        Exibe recomendações organizadas por fator relacionado.

        Args:
            factors (dict): Dicionário de recomendações por fator
        """
        self._print_header("Recomendações por Fator")

        # Adiciona explicação contextual
        print("\nEsta tela mostra as recomendações agrupadas por fator relacionado.")
        print("Cada fator representa um parâmetro ou condição que afeta as perdas")
        print("na colheita mecanizada de cana-de-açúcar.")

        if not factors:
            print("\nNenhuma recomendação específica por fator encontrada!")
            self._wait_keypress()
            return

        # Exibe lista de fatores disponíveis
        print("\nFatores disponíveis:")
        for i, factor in enumerate(sorted(factors.keys()), 1):
            num_recs = len(factors[factor])
            print(f"{i}. {factor} ({num_recs} recomendações)")

        # Solicita escolha do fator
        choice = self._input_with_prompt(
            f"\nEscolha um fator (1-{len(factors)}) ou 0 para cancelar",
            "0"
        )

        # Verifica se a escolha é válida
        try:
            choice_idx = int(choice)
            if choice_idx == 0:
                return

            if 1 <= choice_idx <= len(factors):
                selected_factor = sorted(factors.keys())[choice_idx - 1]
                self._display_factor_recommendations(selected_factor,
                                                factors[selected_factor])
            else:
                print("\nFator inválido!")
                self._wait_keypress()
        except ValueError:
            print("\nOpção inválida! Digite um número.")
            self._wait_keypress()


    def _display_factor_recommendations(self, factor, recommendations):
        """
        Exibe detalhes das recomendações para um fator específico.

        Args:
            factor (str): Nome do fator
            recommendations (list): Lista de recomendações para o fator
        """
        self._print_header(f"Recomendações para {factor.upper()}")

        # Adiciona explicação contextual
        print(f"\nEsta tela mostra todas as recomendações relacionadas ao fator")
        print(f"'{factor}', organizadas por prioridade.")

        # Descrição do fator com base no nome
        factor_descriptions = {
            'harvester_speed': ("Velocidade da colheitadeira - Afeta diretamente "
                            "a qualidade de corte e o nível de perdas. "
                            "Velocidade ideal: 4,5-6,5 km/h."),
            'cutting_height': ("Altura de corte - Determina a quantidade de "
                            "material colhido e perdas. "
                            "Altura ideal: 20-30 mm."),
            'soil_humidity': ("Umidade do solo - Influencia a eficiência da "
                        "colheita e a contaminação do material. "
                        "Umidade ideal: 25-45%."),
            'temperature': ("Temperatura ambiente - Afeta o desempenho de "
                        "equipamentos e operadores. "
                        "Temperatura ideal: 20-30°C."),
            'wind_speed': ("Velocidade do vento - Impacta a precisão da "
                        "colheita e pode aumentar perdas. "
                        "Velocidade ideal: 0-10 km/h.")
        }

        # Exibe descrição do fator se disponível
        if factor in factor_descriptions:
            print(f"\nDESCRIÇÃO DO FATOR:")
            print(f"{factor_descriptions[factor]}")

        # Agrupa por prioridade
        by_priority = {
            'high': [],
            'medium': [],
            'low': []
        }

        for rec in recommendations:
            priority = rec.get('priority', 'low')
            by_priority[priority].append(rec)

        # Exibe recomendações de alta prioridade
        if by_priority['high']:
            self._print_section("ALTA PRIORIDADE (!)")
            for i, rec in enumerate(by_priority['high'], 1):
                text = rec.get('text', '')
                print(f"{i}. {text}")

        # Exibe recomendações de média prioridade
        if by_priority['medium']:
            self._print_section("MÉDIA PRIORIDADE (△)")
            for i, rec in enumerate(by_priority['medium'], 1):
                text = rec.get('text', '')
                print(f"{i}. {text}")

        # Exibe recomendações de baixa prioridade
        if by_priority['low']:
            self._print_section("BAIXA PRIORIDADE (○)")
            for i, rec in enumerate(by_priority['low'], 1):
                text = rec.get('text', '')
                print(f"{i}. {text}")

        # Adiciona legenda
        print("\nLEGENDA DE PRIORIDADE:")
        print("! Alta | △ Média | ○ Baixa")

        self._wait_keypress()



    def _view_general_recommendations(self, recommendations):
        """
        Exibe recomendações gerais não relacionadas a fatores específicos.

        Args:
            recommendations (list): Lista de recomendações gerais
        """
        self._print_header("Recomendações Gerais")

        # Adiciona explicação contextual
        print("\nEsta tela mostra recomendações gerais para redução de perdas")
        print("na colheita. Estas são boas práticas que melhoram a eficiência")
        print("independentemente dos fatores específicos detectados.")

        if not recommendations:
            print("\nNenhuma recomendação geral encontrada!")
            self._wait_keypress()
            return

        # Agrupa por prioridade
        by_priority = {
            'high': [],
            'medium': [],
            'low': []
        }

        for rec in recommendations:
            priority = rec.get('priority', 'low')
            by_priority[priority].append(rec)

        # Exibe recomendações por prioridade
        priorities = ['high', 'medium', 'low']
        priority_titles = {
            'high': 'ALTA PRIORIDADE (!)',
            'medium': 'MÉDIA PRIORIDADE (△)',
            'low': 'BAIXA PRIORIDADE (○)'
        }

        for priority in priorities:
            if by_priority[priority]:
                self._print_section(priority_titles[priority])
                for i, rec in enumerate(by_priority[priority], 1):
                    text = rec.get('text', '')
                    print(f"{i}. {text}")

        # Adiciona legenda
        print("\nLEGENDA DE PRIORIDADE:")
        print("! Alta | △ Média | ○ Baixa")

        self._wait_keypress()


    def _view_all_recommendations(self, recommendations):
        """
        Exibe todas as recomendações em uma única listagem.

        Args:
            recommendations (list): Lista completa de recomendações
        """
        self._print_header("Todas as Recomendações")

        # Adiciona explicação contextual
        print("\nEsta tela mostra todas as recomendações disponíveis para")
        print("redução de perdas na colheita mecanizada de cana-de-açúcar.")

        if not recommendations:
            print("\nNenhuma recomendação encontrada!")
            self._wait_keypress()
            return

        # Ordena por prioridade (alta → média → baixa)
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        sorted_recs = sorted(
            recommendations,
            key=lambda x: priority_order.get(x.get('priority', 'low'), 3)
        )

        # Exibe todas as recomendações
        print("\nRECOMENDAÇÕES ORDENADAS POR PRIORIDADE:")
        print("-" * 70)
        print("Prioridade | Fator       | Recomendação")
        print("-" * 70)

        for rec in sorted_recs:
            priority = rec.get('priority', 'low')
            factor = rec.get('factor', 'geral')
            text = rec.get('text', '')

            # Formata indicador de prioridade
            if priority == 'high':
                priority_text = "!  ALTA   "
            elif priority == 'medium':
                priority_text = "△  MÉDIA  "
            else:
                priority_text = "○  BAIXA  "

            # Trunca texto longo para caber na tela
            if len(text) > 40:
                display_text = text[:37] + "..."
            else:
                display_text = text

            print(f"{priority_text} | {factor[:10]:<10} | {display_text}")

        # Opção para ver detalhes
        print("\n\nOPÇÕES:")
        print("1. Ver detalhes de uma recomendação específica")
        print("2. Voltar")

        choice = self._input_with_prompt("\nEscolha uma opção", "2")

        if choice == '1':
            rec_idx = self._input_with_prompt(
                f"Número da recomendação (1-{len(sorted_recs)})",
                "1"
            )

            try:
                idx = int(rec_idx) - 1
                if 0 <= idx < len(sorted_recs):
                    self._display_recommendation_details(sorted_recs[idx])
                else:
                    print("\nNúmero de recomendação inválido!")
                    self._wait_keypress()
            except ValueError:
                print("\nOpção inválida! Digite um número.")
                self._wait_keypress()


    def _display_recommendation_details(self, recommendation):
        """
        Exibe detalhes completos de uma recomendação específica.

        Args:
            recommendation (dict): Dados da recomendação
        """
        self._print_header("Detalhes da Recomendação")

        # Extrai dados da recomendação
        priority = recommendation.get('priority', 'low')
        factor = recommendation.get('factor', 'geral')
        text = recommendation.get('text', '')

        # Indicador visual de prioridade
        if priority == 'high':
            priority_indicator = "! ALTA PRIORIDADE"
        elif priority == 'medium':
            priority_indicator = "△ MÉDIA PRIORIDADE"
        else:
            priority_indicator = "○ BAIXA PRIORIDADE"

        # Exibe detalhes formatados
        print(f"\nPRIORIDADE: {priority_indicator}")
        print(f"FATOR: {factor.upper()}")
        print(f"\nRECOMENDAÇÃO:")
        print(f"{text}")

        # Adiciona descrição do fator se disponível
        factor_descriptions = {
            'harvester_speed': ("A velocidade da colheitadeira afeta diretamente "
                            "a qualidade de corte e o nível de perdas. "
                            "Velocidades muito altas causam corte irregular e "
                            "desprendimento excessivo. Velocidades muito baixas "
                            "reduzem a eficiência e podem aumentar danos à soqueira."),
            'cutting_height': ("A altura de corte é crucial para minimizar perdas. "
                            "Corte muito alto deixa tocos com açúcar. "
                            "Corte muito baixo aumenta impurezas minerais e "
                            "prejudica rebrota."),
            'soil_humidity': ("A umidade do solo afeta trafegabilidade e qualidade "
                        "de corte. Solo muito seco aumenta impurezas. "
                        "Solo muito úmido causa compactação e prejudica "
                        "eficiência da colheita."),
            'temperature': ("A temperatura afeta desempenho de máquinas e "
                        "operadores. Temperaturas muito altas aumentam perdas "
                        "por dessecação e reduzem eficiência de colheita."),
            'wind_speed': ("Vento forte prejudica o direcionamento preciso da "
                        "colheita e aumenta perdas por dispersão de material.")
        }

        if factor in factor_descriptions:
            print(f"\nDETALHES DO FATOR '{factor.upper()}':")
            print(f"{factor_descriptions[factor]}")

        # Adiciona dicas de implementação
        if priority == 'high':
            print("\nIMPLEMENTAÇÃO RECOMENDADA:")
            print("• Implementar imediatamente")
            print("• Verificar resultados após implementação")
            print("• Documentar mudanças realizadas")

        # Adiciona legenda
        print("\nLEGENDA DE PRIORIDADE:")
        print("! Alta | △ Média | ○ Baixa")

        self._wait_keypress()


    def _get_recommendations_summary(self, recommendations):
        """
        Gera um resumo estatístico das recomendações disponíveis.

        Args:
            recommendations (list): Lista de recomendações

        Returns:
            dict: Estatísticas sobre as recomendações
        """
        if not recommendations:
            return None

        # Contagem por prioridade
        by_priority = {
            'high': 0,
            'medium': 0,
            'low': 0
        }

        # Contagem por fator
        by_factor = {}

        # Contagem por tipo (específico vs geral)
        specific_count = 0
        general_count = 0

        for rec in recommendations:
            # Conta por prioridade
            priority = rec.get('priority', 'low')
            by_priority[priority] += 1

            # Conta por fator
            factor = rec.get('factor', 'general')
            if factor not in by_factor:
                by_factor[factor] = 0
            by_factor[factor] += 1

            # Específico vs geral
            if factor == 'general':
                general_count += 1
            else:
                specific_count += 1

        return {
            'total': len(recommendations),
            'by_priority': by_priority,
            'by_factor': by_factor,
            'specific_count': specific_count,
            'general_count': general_count
        }


    def _export_recommendations(self, recommendations):
        """
        Exporta recomendações para arquivo JSON.

        Permite salvar as recomendações atuais em um formato que pode
        ser importado por outros sistemas ou analisado externamente.

        Args:
            recommendations (list): Lista de recomendações
        """
        if not recommendations:
            print("\nNenhuma recomendação para exportar!")
            self._wait_keypress()
            return

        self._print_header("Exportar Recomendações")

        # Adiciona explicação contextual
        print("\nEsta função exporta as recomendações para um arquivo JSON")
        print("que pode ser importado por outros sistemas ou analisado")
        print("externamente.")

        # Verifica se diretório existe
        export_dir = os.path.join(self.json_manager.base_path, 'exports')
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        # Gera nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"recommendations_{self.session_id}_{timestamp}.json"
        filepath = os.path.join(export_dir, filename)

        # Prepara dados para exportação
        export_data = {
            'session_id': self.session_id,
            'timestamp': datetime.now().isoformat(),
            'recommendations': recommendations,
            'summary': self._get_recommendations_summary(recommendations)
        }

        # Salva arquivo
        try:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2)

            print(f"\nRecomendações exportadas com sucesso para:")
            print(f"{filepath}")

        except Exception as e:
            print(f"\nErro ao exportar recomendações: {str(e)}")

        self._wait_keypress()


    def _filter_recommendations(self, recommendations):
        """
        Permite ao usuário filtrar recomendações por critérios específicos.

        Args:
            recommendations (list): Lista completa de recomendações

        Returns:
            list: Recomendações filtradas
        """
        self._print_header("Filtrar Recomendações")

        if not recommendations:
            print("\nNenhuma recomendação para filtrar!")
            self._wait_keypress()
            return []

        # Adiciona explicação contextual
        print("\nEsta tela permite filtrar as recomendações por diversos critérios")
        print("para focar nas mais relevantes para sua situação específica.")

        # Menu de filtros
        print("\nFILTROS DISPONÍVEIS:")
        print("1. Por prioridade")
        print("2. Por fator relacionado")
        print("3. Apenas recomendações específicas")
        print("4. Apenas recomendações gerais")
        print("5. Cancelar filtro")

        choice = self._input_with_prompt("\nEscolha um filtro", "5")

        filtered_recs = []

        if choice == '1':
            # Filtro por prioridade
            print("\nPRIORIDADES:")
            print("1. Alta prioridade")
            print("2. Média prioridade")
            print("3. Baixa prioridade")

            priority_choice = self._input_with_prompt("\nEscolha a prioridade", "1")

            priority_map = {
                '1': 'high',
                '2': 'medium',
                '3': 'low'
            }

            if priority_choice in priority_map:
                selected_priority = priority_map[priority_choice]
                filtered_recs = [r for r in recommendations
                            if r.get('priority') == selected_priority]

        elif choice == '2':
            # Filtro por fator
            factors = set()
            for rec in recommendations:
                factor = rec.get('factor')
                if factor and factor != 'general':
                    factors.add(factor)

            if not factors:
                print("\nNenhum fator específico encontrado!")
                self._wait_keypress()
                return []

            # Exibe lista de fatores
            print("\nFATORES DISPONÍVEIS:")
            for i, factor in enumerate(sorted(factors), 1):
                print(f"{i}. {factor}")

            factor_choice = self._input_with_prompt(
                f"\nEscolha um fator (1-{len(factors)})",
                "1"
            )

            try:
                idx = int(factor_choice) - 1
                if 0 <= idx < len(factors):
                    selected_factor = sorted(factors)[idx]
                    filtered_recs = [r for r in recommendations
                                if r.get('factor') == selected_factor]
                else:
                    print("\nFator inválido!")
                    self._wait_keypress()
                    return []
            except ValueError:
                print("\nOpção inválida!")
                self._wait_keypress()
                return []

        elif choice == '3':
            # Apenas recomendações específicas
            filtered_recs = [r for r in recommendations
                        if r.get('factor') != 'general']

        elif choice == '4':
            # Apenas recomendações gerais
            filtered_recs = [r for r in recommendations
                        if r.get('factor') == 'general']

        elif choice == '5':
            # Cancelar filtro
            return recommendations
        else:
            print("\nOpção inválida!")
            self._wait_keypress()
            return recommendations

        # Informa resultado do filtro
        print(f"\nEncontradas {len(filtered_recs)} recomendações com este filtro.")
        self._wait_keypress()

        return filtered_recs



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
