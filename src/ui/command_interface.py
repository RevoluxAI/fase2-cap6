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
        self._print_header("Parâmetros Críticos para Perdas na Colheita")

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
        self._print_header("Histórico de Sensor")

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
        self._print_header("Tendências e Estatísticas")

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
