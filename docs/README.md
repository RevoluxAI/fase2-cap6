> Especificação Técnica
# Sistema de Redução de Perdas na Colheita Mecanizada de Cana-de-Açúcar com Monitoramento de GHG

## 1. Visão Geral

Este documento apresenta a especificação técnica para um sistema computacional que visa reduzir as perdas durante a colheita mecanizada da cana-de-açúcar e monitorar as emissões de gases de efeito estufa (GHG), alinhado com o GHG Protocol Agricultural Guidance. O sistema utilizará simuladores de sensores para coletar dados, processá-los e gerar recomendações baseadas em evidências, além de produzir inventários de GHG conformes com padrões internacionais.

## 2. Contextualização do Problema

O Brasil lidera mundialmente a produção de cana-de-açúcar, colhendo aproximadamente 620 milhões de toneladas por safra. Entretanto, a colheita mecanizada apresenta perdas significativas que podem atingir 15% da produção total. Simultaneamente, o setor agrícola brasileiro contribui significativamente para as emissões de GHG, sendo responsável por aproximadamente 11% das emissões antropogênicas globais totais, 60% das emissões de óxido nitroso (N₂O) e 50% das emissões de metano (CH₄).

## 3. Objetivos

### 3.1. Objetivo Principal
Desenvolver uma aplicação em Python que simule, detecte, analise e recomende ações práticas para reduzir perdas na colheita mecanizada da cana-de-açúcar, enquanto monitora e relata emissões de GHG conforme o GHG Protocol Agricultural Guidance.

### 3.2. Objetivos Específicos
- Implementar simuladores de sensores para monitoramento de variáveis operacionais e ambientais
- Processar e analisar dados utilizando estruturas de dados eficientes
- Estimar emissões de GHG de fontes mecânicas e não-mecânicas
- Contabilizar mudanças em estoques de carbono no sistema agrícola
- Persistir informações em arquivos JSON e banco de dados Oracle
- Gerar inventários de GHG compatíveis com padrões internacionais
- Identificar fatores correlacionados com maiores taxas de perda na colheita
- Recomendar ajustes operacionais para redução de perdas e mitigação de emissões

## 4. Arquitetura do Sistema

### 4.1. Visão Arquitetural
O sistema seguirá uma arquitetura em camadas, composta por:

1. **Camada de Simulação**: Responsável pela geração de dados simulados dos sensores
2. **Camada de Inventário GHG**: Implementa protocolos e metodologias de contabilização
3. **Camada de Processamento**: Implementa algoritmos de análise e correlação de dados
4. **Camada de Persistência**: Gerencia o armazenamento e recuperação de dados
5. **Camada de Interface**: Fornece interação com o usuário via terminal de comando

### 4.2. Componentes Principais

#### 4.2.1. Simulador de Sensores
- Simulador de umidade do solo (%)
- Simulador de velocidade da colheitadeira (km/h)
- Simulador de altura de corte basal (mm)
- Simulador de emissões de gases (CH₄ e NH₃)
- Simulador de detecção visual baseado em dados históricos

#### 4.2.2. Contabilizador de GHG
- Módulo de definição de limites organizacionais e operacionais
- Módulo de cálculo de emissões de Escopo 1 (diretas)
- Módulo de cálculo de emissões de Escopo 2 (energia adquirida)
- Módulo de cálculo de emissões de Escopo 3 (opcional, outras indiretas)
- Módulo de contabilização de estoques de carbono

#### 4.2.3. Processador de Dados
- Módulo de normalização e limpeza de dados
- Módulo de detecção de anomalias
- Módulo de correlação entre variáveis operacionais, perdas e emissões
- Módulo de recomendações baseado em regras e histórico

#### 4.2.4. Sistema de Persistência
- Gerenciador de arquivos JSON
- Conector de banco de dados Oracle

#### 4.2.5. Interface com Usuário
- Menu interativo via terminal
- Visualizador de dados e estatísticas
- Gerador de relatórios de perdas e inventários de GHG

## 5. Especificação Técnica

### 5.1. Requisitos Funcionais

1. **RF01** - O sistema deve simular sensores de campo com variação realista de parâmetros
2. **RF02** - O sistema deve calcular estimativas de perda baseadas nos dados dos sensores
3. **RF03** - O sistema deve identificar correlações entre parâmetros operacionais e perdas
4. **RF04** - O sistema deve definir limites organizacionais e operacionais para inventário GHG
5. **RF05** - O sistema deve calcular emissões de Escopo 1 e 2 conforme GHG Protocol
6. **RF06** - O sistema deve contabilizar mudanças em estoques de carbono
7. **RF07** - O sistema deve armazenar dados históricos em arquivos JSON
8. **RF08** - O sistema deve sincronizar dados críticos com banco Oracle
9. **RF09** - O sistema deve recomendar ajustes operacionais para minimizar perdas e emissões
10. **RF10** - O sistema deve gerar relatórios compatíveis com o GHG Protocol Agricultural Guidance

### 5.2. Requisitos Não-Funcionais

1. **RNF01** - O código deve seguir o limite de 80 colunas
2. **RNF02** - Os comentários devem utilizar voz ativa
3. **RNF03** - Os comentários devem utilizar particípio na terceira pessoa do singular
4. **RNF04** - O sistema deve incluir testes unitários para validação de funcionalidades
5. **RNF05** - O sistema deve ser executável em ambiente Python 3.8 ou superior
6. **RNF06** - O sistema deve responder a comandos do usuário em menos de 1 segundo
7. **RNF07** - A interface deve apresentar informações de forma clara e objetiva
8. **RNF08** - Os cálculos de GHG devem seguir metodologias reconhecidas (IPCC Tier 1, 2 ou 3)
9. **RNF09** - Os relatórios devem atender aos princípios de relevância, completude, consistência, transparência e precisão do GHG Protocol

### 5.3. Estruturas de Dados

#### 5.3.1. Listas
Utilizadas para séries temporais de dados dos sensores e histórico de recomendações.

```python
# Exemplo de estrutura para série temporal de um sensor
sensor_data = [
    {'timestamp': '2025-04-15T10:30:00', 'value': 45.7, 'unit': '%'},
    {'timestamp': '2025-04-15T10:31:00', 'value': 46.2, 'unit': '%'}
]
```

#### 5.3.2. Tuplas
Utilizadas para configurações fixas e constantes do sistema.

```python
# Exemplo de configuração de limites operacionais
OPERATION_LIMITS = (
    ('soil_humidity', 30, 60, '%'),
    ('harvester_speed', 4, 8, 'km/h'),
    ('cutting_height', 20, 40, 'mm')
)

# Exemplo de fatores de emissão GHG
GHG_EMISSION_FACTORS = (
    ('diesel_fuel',  2.68,  'kg CO2e/L'),
    ('fertilizer_n', 4.87, 'kg CO2e/kg N'),
    ('soil_organic', 0.3,  'kg N2O-N/kg N')
)
```

#### 5.3.3. Dicionários
Utilizados para armazenar configurações, parâmetros operacionais e resultados de análises.

```python
# Exemplo de configuração de simulação e limites de inventário
ghg_inventory_config = {
    'organizational_boundary': 'operational_control',
    'base_period': '2025',
    'included_scopes': ['scope1', 'scope2'],
    'carbon_stocks': {
        'soil_organic_carbon': True,
        'above_ground_biomass': True,
        'below_ground_biomass': True,
        'dead_organic_matter': True
    },
    'ghg_gases': ['CO2', 'CH4', 'N2O'],
    'calculation_tier': 'tier2'
}
```

#### 5.3.4. Tabelas de Memória
Implementadas como classes para análise multidimensional dos dados.

```python
class EmissionsTable:
    def __init__(self):
        self.columns = []
        self.data = []
        self.scope = None
        self.source_category = None

    def set_scope(self, scope):
        """
        Define o escopo das emissões (1, 2 ou 3).

        Args:
            scope (int): Escopo GHG Protocol
        """
        self.scope = scope

    def set_source_category(self, category):
        """
        Define a categoria da fonte de emissão.

        Args:
            category (str): Categoria (ex: 'mechanical', 'non-mechanical')
        """
        self.source_category = category

    def add_column(self, name, data_type, unit=None):
        """
        Adiciona coluna à tabela.

        Args:
            name (str): Nome da coluna
            data_type (str): Tipo de dados
            unit (str): Unidade de medida
        """
        self.columns.append({'name': name, 'type': data_type, 'unit': unit})

    def add_row(self, row_data):
        """
        Adiciona linha de dados à tabela.

        Args:
            row_data (list): Dados da linha
        """
        if len(row_data) != len(self.columns):
            raise ValueError("Dados incompatíveis com estrutura da tabela")
        self.data.append(row_data)
```

### 5.4. Persistência de Dados

#### 5.4.1. Formato JSON
Utilizado para armazenamento local de configurações e dados históricos.

```json
{
  "session_id": "20250415-143022",
  "ghg_inventory": {
    "organizational_boundary": "operational_control",
    "operational_boundary": {
      "scope1": true,
      "scope2": true,
      "scope3": false
    },
    "base_period": "2025",
    "reporting_period": "2025-04-15"
  },
  "emissions": {
    "scope1": {
      "mechanical": [
        {"source": "diesel_combustion", "value": 250.5, "unit": "kg CO2e"},
        {"source": "harvester", "value": 120.3, "unit": "kg CO2e"}
      ],
      "non_mechanical": [
        {"source": "soil_management", "value": 180.7, "unit": "kg CO2e"},
        {"source": "residue_burning", "value": 90.2, "unit": "kg CO2e"}
      ]
    },
    "scope2": [
      {"source": "purchased_electricity", "value": 150.0, "unit": "kg CO2e"}
    ]
  },
  "carbon_stocks": {
    "soil_organic_carbon": {"change": -15.3, "unit": "kg CO2e"},
    "above_ground_biomass": {"change": 10.5, "unit": "kg CO2e"},
    "below_ground_biomass": {"change": 5.2, "unit": "kg CO2e"}
  },
  "harvest_losses": {
    "total": 12.5,
    "unit": "%",
    "contributing_factors": ["high_speed", "low_cutting_height"]
  }
}
```

#### 5.4.2. Banco de Dados Oracle
Utilizado para armazenamento persistente e análises avançadas.

```sql
-- Exemplo de esquema de banco de dados ampliado
CREATE TABLE sessions (
    session_id VARCHAR2(20) PRIMARY KEY,
    timestamp TIMESTAMP,
    org_boundary VARCHAR2(30),
    base_period VARCHAR2(10)
);

CREATE TABLE sensor_data (
    id NUMBER GENERATED ALWAYS AS IDENTITY,
    session_id VARCHAR2(20),
    timestamp TIMESTAMP,
    sensor_type VARCHAR2(30),
    sensor_value NUMBER(10,2),
    unit VARCHAR2(10),
    PRIMARY KEY (id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE ghg_emissions (
    id NUMBER GENERATED ALWAYS AS IDENTITY,
    session_id VARCHAR2(20),
    timestamp TIMESTAMP,
    scope NUMBER(1),
    category VARCHAR2(30),
    source VARCHAR2(50),
    gas VARCHAR2(10),
    value NUMBER(10,2),
    unit VARCHAR2(10),
    calculation_method VARCHAR2(20),
    PRIMARY KEY (id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE carbon_stocks (
    id NUMBER GENERATED ALWAYS AS IDENTITY,
    session_id VARCHAR2(20),
    timestamp TIMESTAMP,
    stock_type VARCHAR2(30),
    change NUMBER(10,2),
    amortization_period NUMBER(3),
    unit VARCHAR2(10),
    PRIMARY KEY (id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE harvest_losses (
    id NUMBER GENERATED ALWAYS AS IDENTITY,
    session_id VARCHAR2(20),
    timestamp TIMESTAMP,
    loss_percent NUMBER(5,2),
    factors VARCHAR2(100),
    PRIMARY KEY (id),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
```

## 6. Implementação

### 6.1. Estrutura de Diretórios

```
cana-loss-reduction/
│
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── simulation/
│   │   ├── __init__.py
│   │   ├── sensor_simulator.py
│   │   └── weather_simulator.py
│   ├── ghg_inventory/
│   │   ├── __init__.py
│   │   ├── boundary_manager.py
│   │   ├── emissions_calculator.py
│   │   ├── carbon_stock_manager.py
│   │   └── reporting_engine.py
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── data_analyzer.py
│   │   └── recommendation_engine.py
│   ├── persistence/
│   │   ├── __init__.py
│   │   ├── json_manager.py
│   │   └── oracle_connector.py
│   └── ui/
│       ├── __init__.py
│       └── command_interface.py
│
├── tests/
│   ├── __init__.py
│   ├── test_sensor_simulator.py
│   ├── test_emissions_calculator.py
│   ├── test_carbon_stock_manager.py
│   ├── test_data_analyzer.py
│   ├── test_json_manager.py
│   └── test_oracle_connector.py
│
├── data/
│   ├── config.json
│   ├── simulation_results/
│   └── historical_data/
│
├── docs/
│   ├── README.md
│   └── architecture.md
│
└── requirements.txt
```

### 6.2. Dependências

```
# requirements.txt
numpy==1.22.3
pandas==1.4.2
matplotlib==3.5.1
cx_Oracle==8.3.0
pytest==7.1.1
```

### 6.3. Módulos Principais

#### 6.3.1. Gerenciador de Limites GHG

```python
# src/ghg_inventory/boundary_manager.py
class BoundaryManager:
    """
    Gerencia limites organizacionais e operacionais do inventário GHG.
    """

    def __init__(self, config):
        """
        Inicializa o gerenciador de limites.

        Args:
            config (dict): Configurações de limites
        """
        self.config = config
        self.org_boundary_approach = self.config.get(
            'organizational_boundary', 'operational_control')
        self.scope1_enabled = True  # Sempre requerido pelo GHG Protocol
        self.scope2_enabled = True  # Sempre requerido pelo GHG Protocol
        self.scope3_enabled = self.config.get('include_scope3', False)
        self.base_period = self.config.get('base_period', '2025')

    def is_source_included(self, source, scope):
        """
        Verifica se uma fonte de emissão está dentro dos limites.

        Args:
            source (str): Nome da fonte de emissão
            scope (int): Escopo da fonte (1, 2 ou 3)

        Returns:
            bool: Indica se a fonte está incluída no inventário
        """
        if scope == 1 and self.scope1_enabled:
            return True
        if scope == 2 and self.scope2_enabled:
            return True
        if scope == 3 and self.scope3_enabled:
            # Verifica se a fonte específica de escopo 3 está habilitada
            scope3_sources = self.config.get('scope3_sources', [])
            return source in scope3_sources

        return False

    def get_operational_control_entities(self):
        """
        Retorna entidades sob controle operacional.

        Returns:
            list: Entidades dentro do limite organizacional
        """
        if self.org_boundary_approach == 'operational_control':
            return self.config.get('controlled_entities', [])
        return []

    def get_financial_control_entities(self):
        """
        Retorna entidades sob controle financeiro.

        Returns:
            list: Entidades dentro do limite organizacional
        """
        if self.org_boundary_approach == 'financial_control':
            return self.config.get('controlled_entities', [])
        return []

    def get_equity_share_entities(self):
        """
        Retorna entidades com participação acionária.

        Returns:
            dict: Entidades e percentuais de participação
        """
        if self.org_boundary_approach == 'equity_share':
            return self.config.get('equity_entities', {})
        return {}
```

#### 6.3.2. Calculador de Emissões

```python
# src/ghg_inventory/emissions_calculator.py
class EmissionsCalculator:
    """
    Calcula emissões de GHG para diferentes fontes e escopos.
    """

    def __init__(self, boundary_manager, emission_factors=None):
        """
        Inicializa o calculador de emissões.

        Args:
            boundary_manager: Gerenciador de limites GHG
            emission_factors (dict): Fatores de emissão
        """
        self.boundary_manager = boundary_manager
        self.emission_factors = emission_factors or {}
        self.calculation_tier = boundary_manager.config.get('calculation_tier',
                                                           'tier1')

    def calculate_mechanical_emissions(self, fuel_data):
        """
        Calcula emissões de fontes mecânicas.

        Args:
            fuel_data (dict): Dados de consumo de combustível

        Returns:
            dict: Emissões por tipo de gás e total em CO2e
        """
        emissions = {
            'CO2': 0.0,
            'CH4': 0.0,
            'N2O': 0.0,
            'CO2e': 0.0
        }

        for fuel_type, amount in fuel_data.items():
            if fuel_type in self.emission_factors:
                factor = self.emission_factors[fuel_type]
                emissions['CO2'] += amount * factor.get('CO2', 0)
                emissions['CH4'] += amount * factor.get('CH4', 0)
                emissions['N2O'] += amount * factor.get('N2O', 0)
                emissions['CO2e'] += amount * factor.get('CO2e', 0)

        return emissions

    def calculate_non_mechanical_emissions(self, activity_data):
        """
        Calcula emissões de fontes não-mecânicas.

        Args:
            activity_data (dict): Dados de atividade agrícola

        Returns:
            dict: Emissões por tipo de gás e total em CO2e
        """
        emissions = {
            'CO2': 0.0,
            'CH4': 0.0,
            'N2O': 0.0,
            'CO2e': 0.0
        }

        # Implementa cálculos específicos para cada fonte não-mecânica
        # Usando metodologias IPCC apropriadas ao tier configurado
        if 'soil_management' in activity_data:
            soil_data = activity_data['soil_management']
            emissions['N2O'] += self._calculate_soil_n2o(soil_data)

        if 'residue_burning' in activity_data:
            burning_data = activity_data['residue_burning']
            ch4, n2o = self._calculate_burning_emissions(burning_data)
            emissions['CH4'] += ch4
            emissions['N2O'] += n2o

        # Calcula CO2e total usando GWP apropriados
        emissions['CO2e'] = (emissions['CO2'] +
                            emissions['CH4'] * 28 +
                            emissions['N2O'] * 265)

        return emissions

    def _calculate_soil_n2o(self, soil_data):
        """
        Calcula emissões de N2O de solos agrícolas.

        Args:
            soil_data (dict): Dados de manejo do solo

        Returns:
            float: Emissões de N2O em kg
        """
        # Implementa cálculos conforme metodologia IPCC
        # Este é apenas um esqueleto simplificado
        if self.calculation_tier == 'tier1':
            # Usa fator de emissão padrão IPCC Tier 1
            ef = 0.01  # kg N2O-N / kg N aplicado
            n_applied = soil_data.get('nitrogen_applied', 0)
            return n_applied * ef * 44/28  # Conversão N2O-N para N2O

        # Implementações para Tier 2 e 3 seriam mais complexas
        return 0.0

    def _calculate_burning_emissions(self, burning_data):
        """
        Calcula emissões de CH4 e N2O da queima de resíduos.

        Args:
            burning_data (dict): Dados de queima de resíduos

        Returns:
            tuple: Emissões de CH4 e N2O em kg
        """
        # Implementa cálculos conforme metodologia IPCC
        # Este é apenas um esqueleto simplificado
        if self.calculation_tier == 'tier1':
            # Usar fatores de emissão padrão IPCC Tier 1
            biomass = burning_data.get('biomass_burned', 0)
            combustion_factor = 0.80  # Fração da biomassa que queima
            ch4_ef = 2.7  # g CH4 / kg matéria seca queimada
            n2o_ef = 0.07  # g N2O / kg matéria seca queimada

            ch4 = biomass * combustion_factor * ch4_ef / 1000  # kg
            n2o = biomass * combustion_factor * n2o_ef / 1000  # kg

            return ch4, n2o

        # Implementações para Tier 2 e 3 seriam mais complexas
        return 0.0, 0.0
```

#### 6.3.3. Gerenciador de Estoques de Carbono

```python
# src/ghg_inventory/carbon_stock_manager.py
class CarbonStockManager:
    """
    Gerencia contabilização de estoques de carbono.
    """

    def __init__(self, config):
        """
        Inicializa o gerenciador de estoques de carbono.

        Args:
            config (dict): Configurações para gestão de estoques
        """
        self.config = config
        self.amortization_period = config.get('amortization_period', 20)
        self.tracked_stocks = {
            'soil_organic_carbon': config.get('track_soil_carbon', True),
            'above_ground_biomass': config.get('track_above_ground', True),
            'below_ground_biomass': config.get('track_below_ground', True),
            'dead_organic_matter': config.get('track_dom', True)
        }
        self.historical_data = {}

    def calculate_stock_changes(self, current_data, previous_data=None):
        """
        Calcula mudanças em estoques de carbono.

        Args:
            current_data (dict): Dados atuais de estoques
            previous_data (dict): Dados anteriores de estoques

        Returns:
            dict: Mudanças em estoques por categoria
        """
        changes = {}

        for stock_type, is_tracked in self.tracked_stocks.items():
            if not is_tracked:
                continue

            current_value = current_data.get(stock_type, 0)
            previous_value = 0

            if previous_data:
                previous_value = previous_data.get(stock_type, 0)

            # Mudança positiva = sequestro; negativa = emissão
            change = current_value - previous_value

            # Converte C para CO2 multiplicando por 44/12
            change_co2 = change * 44/12

            changes[stock_type] = {
                'change_c': change,
                'change_co2': change_co2,
                'amortization_period': self.amortization_period
            }

        return changes

    def calculate_amortized_flux(self, stock_changes, period=1):
        """
        Calcula fluxo amortizado para o período.

        Args:
            stock_changes (dict): Mudanças em estoques
            period (int): Período específico dentro do período total

        Returns:
            dict: Fluxo amortizado por tipo de estoque
        """
        amortized_flux = {}

        for stock_type, change_data in stock_changes.items():
            total_change = change_data.get('change_co2', 0)
            amort_period = change_data.get('amortization_period',
                                          self.amortization_period)

            # Distribuição linear sobre o período de amortização
            if amort_period > 0:
                amortized_flux[stock_type] = total_change / amort_period
            else:
                amortized_flux[stock_type] = total_change

        return amortized_flux

    def is_land_use_change(self, previous_use, current_use):
        """
        Determina se ocorreu mudança no uso da terra.

        Args:
            previous_use (str): Uso anterior da terra
            current_use (str): Uso atual da terra

        Returns:
            bool: Indica se houve mudança no uso da terra
        """
        land_use_categories = [
            'forest', 'cropland', 'grassland', 'wetland', 'settlements'
        ]

        # Considerado LUC se a categoria mudou
        return (previous_use in land_use_categories and
                current_use in land_use_categories and
                previous_use != current_use)
```

#### 6.3.4. Motor de Relatórios GHG

```python
# src/ghg_inventory/reporting_engine.py
class ReportingEngine:
    """
    Gera relatórios de inventário GHG conforme protocolo.
    """

    def __init__(self, boundary_manager, emissions_calculator,
                carbon_stock_manager):
        """
        Inicializa o motor de relatórios.

        Args:
            boundary_manager: Gerenciador de limites
            emissions_calculator: Calculador de emissões
            carbon_stock_manager: Gerenciador de estoques de carbono
        """
        self.boundary_manager = boundary_manager
        self.emissions_calculator = emissions_calculator
        self.carbon_stock_manager = carbon_stock_manager
        self.inventory_data = {
            'scope1': {'mechanical': {}, 'non_mechanical': {}, 'luc': {}},
            'scope2': {},
            'scope3': {},
            'biogenic_carbon': {
                'land_use_management': {},
                'sequestration_luc': {},
                'biofuel_combustion': {}
            }
        }

    def add_emissions_data(self, scope, category, source, emissions_data):
        """
        Adiciona dados de emissões ao inventário.

        Args:
            scope (int): Escopo das emissões (1, 2 ou 3)
            category (str): Categoria da fonte
            source (str): Nome da fonte
            emissions_data (dict): Dados de emissões
        """
        scope_key = f'scope{scope}'

        if scope == 1:
            if category == 'luc':
                self.inventory_data[scope_key]['luc'][source] = emissions_data
            elif category in ['mechanical', 'non_mechanical']:
                self.inventory_data[scope_key][category][source] = emissions_data
        elif scope == 2:
            self.inventory_data[scope_key][source] = emissions_data
        elif scope == 3:
            self.inventory_data[scope_key][source] = emissions_data

    def add_carbon_flux_data(self, category, stock_type, flux_data):
        """
        Adiciona dados de fluxo de carbono ao inventário.

        Args:
            category (str): Categoria do fluxo
            stock_type (str): Tipo de estoque
            flux_data (dict): Dados do fluxo
        """
        if category in self.inventory_data['biogenic_carbon']:
            self.inventory_data['biogenic_carbon'][category][stock_type] = flux_data

    def generate_inventory_report(self):
        """
        Gera relatório de inventário completo.

        Returns:
            dict: Relatório estruturado conforme GHG Protocol
        """
        report = {
            'inventory_information': {
                'organizational_boundary':
                    self.boundary_manager.org_boundary_approach,
                'operational_boundary': {
                    'scope1': self.boundary_manager.scope1_enabled,
                    'scope2': self.boundary_manager.scope2_enabled,
                    'scope3': self.boundary_manager.scope3_enabled
                },
                'base_period': self.boundary_manager.base_period,
                'calculation_methodology':
                    self.emissions_calculator.calculation_tier
            },
            'emissions_by_scope': self._calculate_totals_by_scope(),
            'emissions_by_gas': self._calculate_totals_by_gas(),
            'emissions_by_source': self._calculate_totals_by_source(),
            'biogenic_carbon': self._calculate_biogenic_carbon()
        }

        return report

    def _calculate_totals_by_scope(self):
        """
        Calcula totais de emissões por escopo.

        Returns:
            dict: Totais por escopo
        """
        totals = {'scope1': 0, 'scope2': 0, 'scope3': 0}

        # Escopo 1
        for category in ['mechanical', 'non_mechanical', 'luc']:
            for source, data in self.inventory_data['scope1'][category].items():
                totals['scope1'] += data.get('CO2e', 0)

        # Escopo 2
        for source, data in self.inventory_data['scope2'].items():
            totals['scope2'] += data.get('CO2e', 0)

        # Escopo 3
        for source, data in self.inventory_data['scope3'].items():
            totals['scope3'] += data.get('CO2e', 0)

        return totals

    def _calculate_totals_by_gas(self):
        """
        Calcula totais de emissões por gás.

        Returns:
            dict: Totais por gás
        """
        # Implementação simplificada
        totals = {'CO2': 0, 'CH4': 0, 'N2O': 0}

        # Implementação completa somaria todos os gases de todos os escopos
        return totals

    def _calculate_totals_by_source(self):
        """
        Calcula totais de emissões por fonte.

        Returns:
            dict: Totais por fonte
        """
        # Implementação simplificada
        totals = {}

        # Implementação completa somaria todas as fontes de todos os escopos
        return totals

    def _calculate_biogenic_carbon(self):
        """
        Calcula totais de fluxos de carbono biogênico.

        Returns:
            dict: Totais de fluxos biogênicos
        """
        totals = {
            'land_use_management': 0,
            'sequestration_luc': 0,
            'biofuel_combustion': 0,
            'total': 0
        }

        for category, stocks in self.inventory_data['biogenic_carbon'].items():
            for stock_type, data in stocks.items():
                value = data.get('flux', 0)
                totals[category] += value
                totals['total'] += value

        return totals
```

#### 6.3.5. Simulador de Sensores

```python
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
```

### 6.4. Testes Unitários

```python
# tests/test_emissions_calculator.py
import unittest
from src.ghg_inventory.boundary_manager import BoundaryManager
from src.ghg_inventory.emissions_calculator import EmissionsCalculator

class TestEmissionsCalculator(unittest.TestCase):
    """
    Testes para o calculador de emissões GHG.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        config = {
            'organizational_boundary': 'operational_control',
            'calculation_tier': 'tier1'
        }
        self.boundary_manager = BoundaryManager(config)

        self.emission_factors = {
            'diesel': {
                'CO2': 2.68,
                'CH4': 0.0001,
                'N2O': 0.0001,
                'CO2e': 2.71
            }
        }

        self.calculator = EmissionsCalculator(
            self.boundary_manager,
            self.emission_factors
        )

    def test_calculate_mechanical_emissions(self):
        """
        Testa cálculo de emissões mecânicas.
        """
        fuel_data = {'diesel': 100.0}  # 100 litros

        emissions = self.calculator.calculate_mechanical_emissions(fuel_data)

        self.assertAlmostEqual(emissions['CO2'], 268.0, places=1)
        self.assertAlmostEqual(emissions['CH4'], 0.01, places=3)
        self.assertAlmostEqual(emissions['N2O'], 0.01, places=3)
        self.assertAlmostEqual(emissions['CO2e'], 271.0, places=1)

    def test_calculate_non_mechanical_emissions_soil(self):
        """
        Testa cálculo de emissões não-mecânicas de solos.
        """
        activity_data = {
            'soil_management': {
                'nitrogen_applied': 100.0  # 100 kg N
            }
        }

        emissions = self.calculator.calculate_non_mechanical_emissions(activity_data)

        # Verifica cálculo de N2O usando fator IPCC Tier 1
        self.assertGreater(emissions['N2O'], 0)
        self.assertGreater(emissions['CO2e'], 0)

    def test_calculate_non_mechanical_emissions_burning(self):
        """
        Testa cálculo de emissões não-mecânicas da queima de resíduos.
        """
        activity_data = {
            'residue_burning': {
                'biomass_burned': 1000.0  # 1000 kg
            }
        }

        emissions = self.calculator.calculate_non_mechanical_emissions(activity_data)

        # Verifica cálculo de CH4 e N2O usando fatores IPCC Tier 1
        self.assertGreater(emissions['CH4'], 0)
        self.assertGreater(emissions['N2O'], 0)
        self.assertGreater(emissions['CO2e'], 0)
```

```python
# tests/test_carbon_stock_manager.py
import unittest
from src.ghg_inventory.carbon_stock_manager import CarbonStockManager

class TestCarbonStockManager(unittest.TestCase):
    """
    Testes para o gerenciador de estoques de carbono.
    """

    def setUp(self):
        """
        Configura ambiente para testes.
        """
        config = {
            'amortization_period': 20,
            'track_soil_carbon': True,
            'track_above_ground': True,
            'track_below_ground': True,
            'track_dom': True
        }
        self.manager = CarbonStockManager(config)

    def test_calculate_stock_changes(self):
        """
        Testa cálculo de mudanças em estoques de carbono.
        """
        current_data = {
            'soil_organic_carbon': 100.0,
            'above_ground_biomass': 50.0,
            'below_ground_biomass': 20.0,
            'dead_organic_matter': 10.0
        }

        previous_data = {
            'soil_organic_carbon': 95.0,
            'above_ground_biomass': 45.0,
            'below_ground_biomass': 18.0,
            'dead_organic_matter': 12.0
        }

        changes = self.manager.calculate_stock_changes(current_data, previous_data)

        # Verificar cálculos para cada tipo de estoque
        self.assertEqual(changes['soil_organic_carbon']['change_c'], 5.0)
        self.assertAlmostEqual(
            changes['soil_organic_carbon']['change_co2'],
            5.0 * 44/12,
            places=2
        )

        self.assertEqual(changes['above_ground_biomass']['change_c'], 5.0)
        self.assertEqual(changes['below_ground_biomass']['change_c'], 2.0)
        self.assertEqual(changes['dead_organic_matter']['change_c'], -2.0)

    def test_calculate_amortized_flux(self):
        """
        Testa cálculo de fluxo amortizado.
        """
        stock_changes = {
            'soil_organic_carbon': {
                'change_c': 5.0,
                'change_co2': 18.33,
                'amortization_period': 20
            },
            'above_ground_biomass': {
                'change_c': -10.0,
                'change_co2': -36.67,
                'amortization_period': 20
            }
        }

        amortized_flux = self.manager.calculate_amortized_flux(stock_changes)

        # Verificar amortização linear
        self.assertAlmostEqual(
            amortized_flux['soil_organic_carbon'],
            18.33 / 20,
            places=2
        )
        self.assertAlmostEqual(
            amortized_flux['above_ground_biomass'],
            -36.67 / 20,
            places=2
        )

    def test_is_land_use_change(self):
        """
        Testa detecção de mudança no uso da terra.
        """
        self.assertTrue(
            self.manager.is_land_use_change('forest', 'cropland')
        )
        self.assertTrue(
            self.manager.is_land_use_change('cropland', 'grassland')
        )
        self.assertFalse(
            self.manager.is_land_use_change('cropland', 'cropland')
        )
        self.assertFalse(
            self.manager.is_land_use_change('unknown', 'cropland')
        )
```

## 7. Cronograma de Desenvolvimento

| Dia | Atividades |
|-----|------------|
| 1-2 | Configuração do ambiente e estrutura base do projeto |
| 3-4 | Implementação dos simuladores de sensores e limites de inventário GHG |
| 5-6 | Implementação do calculador de emissões e gerenciador de estoques de carbono |
| 7-8 | Implementação do processador de dados e sistemas de persistência |
| 9 | Implementação da interface de comando e integração dos módulos |
| 10 | Testes, ajustes finais e documentação |

## 8. Conclusão

Esta especificação técnica revisada detalha um sistema completo para simulação, análise e recomendação de ajustes operacionais visando a redução de perdas na colheita mecanizada da cana-de-açúcar, alinhado com os princípios e metodologias do GHG Protocol Agricultural Guidance.

A implementação proposta não apenas contribuirá para a redução das perdas físicas de produção, mas também permitirá a contabilização precisa das emissões de gases de efeito estufa associadas às operações agrícolas, oferecendo uma solução integrada para a gestão de eficiência operacional e sustentabilidade ambiental.

O sistema possibilitará:
1. Quantificar emissões diretas e indiretas de GHG
2. Contabilizar mudanças em estoques de carbono
3. Gerar relatórios conformes com padrões internacionais
4. Identificar oportunidades de redução de perdas e mitigação de emissões
5. Prever impactos de diferentes práticas de manejo

Esta abordagem multidimensional coloca o projeto na vanguarda da agricultura de precisão, oferecendo ferramentas que potencializam tanto a eficiência econômica quanto a responsabilidade ambiental no setor sucroalcooleiro brasileiro.

Gases usados no cálculo:
- CO2  - Gás Carbônico (dióxido de carbono)
- CH4  - Gás Natural (metano)
- N2O  - Gás Incolor (óxido nitroso)
- CO2e - Gás de Efeito Estufa (dióxido de carbono equivalente)
