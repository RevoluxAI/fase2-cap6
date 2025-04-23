### **Dor**
Emissão de carbono (Em 2019 a atividade rural foi responsável por 72% das emissões no pais. Ref. https://agrotools.com.br/blog/esg-sustentabilidade/agricultura-de-baixo-carbono-desafios-e-oportunidades/)

### **Setor**
Setor de produção

### **Solução**

### 1. **Camada de Coleta de Dados** (Frontend / Entrada de Dados)

Responsável por coletar dados dos agricultores sobre práticas agrícolas e emissão de carbono. A coleta pode ser feita através de uma interface (por exemplo, uma interface web ou uma API) onde os agricultores podem inserir informações.

- **Entradas de Dados**:
    - Informações do agricultor: nome, localização, práticas agrícolas utilizadas.
    - Informações sobre as práticas agrícolas aplicadas: tipos de cultivo, uso de energias renováveis, manejo de resíduos, etc.
    - Medições de emissões de carbono de cada prática agrícola.
  
- **Estruturas de Dados**:
    - **Lista**: Armazena as práticas agrícolas com suas descrições e impactos sobre a emissão de carbono.
    - **Tupla**: Contém as informações do agricultor, como nome, local e práticas adotadas.
    - **Dicionário**: Armazena os dados de emissão de carbono para cada agricultor, calculando o total de emissões e armazenando as recomendações.

### 2. **Camada de Processamento de Dados** (Lógica de Cálculo e Recomendações)

Esta camada recebe os dados coletados e executa o cálculo da emissão de carbono com base nas práticas agrícolas informadas. Além disso, gera recomendações para a redução da emissão de carbono.

#### 2.1 **Função de Cálculo de Carbono**
A função calcula a quantidade total de carbono emitido com base nas práticas agrícolas fornecidas. Ela recebe os dados de entrada, como o tipo de prática agrícola, e calcula a emissão de carbono para cada prática.

#### 2.2 **Função de Recomendação**
O sistema pode gerar recomendações com base nas práticas que emitem maior carbono. Por exemplo:
- Se o agricultor utiliza práticas com grande emissão, o sistema recomenda alternativas como o uso de energias renováveis ou a adoção de sistemas de plantio direto.
  
#### 2.3 **Manipulação de Arquivos (Texto e JSON)**
Uma vez calculadas as emissões e feitas as recomendações, o sistema salva os dados em arquivos **JSON** para armazenamento e compartilhamento posterior.

### 3. **Camada de Armazenamento e Persistência**

Aqui, os dados coletados e calculados são armazenados de forma persistente.

#### 3.1 **Banco de Dados (Oracle)**

Os dados coletados, como as informações do agricultor e as emissões de carbono, são inseridos em um banco de dados Oracle para garantir a persistência a longo prazo. A tabela pode ser estruturada da seguinte maneira:

- **Tabela Agricultores**:
    - ID do agricultor
    - Nome do agricultor
    - Localização
    - Práticas agrícolas adotadas
    - Emissões de carbono calculadas

O banco de dados pode ser utilizado para consultas futuras, análises e para fornecer recomendações personalizadas com base nas práticas históricas.

#### 3.2 **Armazenamento em Arquivos JSON**
Além do banco de dados, o sistema também pode gerar e salvar arquivos **JSON** com os dados dos agricultores e suas práticas agrícolas. Esses arquivos podem ser exportados ou compartilhados com outros sistemas de monitoramento ou organizações ambientais.

### 4. **Fluxo de Dados e Interação entre Componentes**

1. **Coleta de Dados**: O agricultor insere informações sobre suas práticas agrícolas, localização e emissões de carbono.
   
2. **Processamento**: A função calcula a emissão de carbono com base nas práticas informadas. Em seguida, o sistema gera recomendações sobre como reduzir a emissão.

3. **Armazenamento**:
    - Os dados são salvos em arquivos **JSON** para armazenamento local e fácil compartilhamento.
    - Os dados também são enviados e armazenados no banco de dados **Oracle** para persistência e futuras consultas.

4. **Visualização e Relatórios**: Um módulo adicional pode ser utilizado para gerar relatórios para os agricultores, mostrando sua pegada de carbono e fornecendo as melhores práticas recomendadas para reduzir suas emissões.

### 5. **Diagrama:**

<img src="assets/diagram.png" alt="Diagrama da arquitertura de software" >


### 6. **Melhorias**

- **Desempenho e Escalabilidade**: O sistema pode ser expandido para lidar com grandes volumes de dados, implementando técnicas de caching e processamento assíncrono.
- **Segurança**: Garantir a segurança das informações dos agricultores e dados sensíveis, especialmente ao usar a conexão com o banco de dados.
- **Manutenção e Evolução**: O sistema pode ser atualizado facilmente com novas práticas agrícolas ou novos cálculos de emissões.