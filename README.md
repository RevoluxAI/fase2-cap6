# FIAP - Faculdade de Informática e Administração Paulista

<p align="center">
<a href= "https://www.fiap.com.br/"><img src="assets/logo-fiap.png" alt="FIAP - Faculdade de Informática e Admnistração Paulista" border="0" width=40% height=40%></a>
</p>

<br>

# Atividade em grupo - Fase 2 - Cap 6

## RevoluxIA

## 👨‍🎓 Integrantes: 
- <a href="https://www.linkedin.com/in/leonardosena/">Leonardo de Sena - RM563351</a>
- <a href="https://www.linkedin.com/in/moises-cavalcante-aaab24142/">Moises de Lima Cavalcante - RM561909</a>
- <a href="https://www.linkedin.com/in/ricardobsoares/">Ricardo Borges Soares - RM561421</a> 
- <a href="https://www.linkedin.com/in/vivian-amorim-245a46b7/">Vivian Nascimento Silva Amorim - RM565078</a> 

## 👩‍🏫 Professores:
### Tutor(a) 
- <a href="https://www.linkedin.com/in/leonardoorabona/">Leonardo Ruiz Orabona</a>
### Coordenador(a)
- <a href="https://www.linkedin.com/company/inova-fusca">André Godoi Chiovato</a>


## 📜 Descrição

### Problema
Em 2019, a atividade rural foi responsável por 72% das emissões de carbono no Brasil, uma contribuição significativa para as mudanças climáticas. O desafio está em transformar essas práticas agrícolas para que se tornem mais sustentáveis, reduzindo as emissões de gases de efeito estufa e promovendo um ambiente mais saudável. Esse cenário exige uma solução eficiente para monitorar e controlar as emissões de carbono no setor agropecuário.

### Setor de Atuação
O setor em questão é o de produção agrícola. As práticas adotadas por agricultores, como uso de fertilizantes, manejo do solo e escolha de cultivos, têm um impacto direto nas emissões de carbono. Assim, o setor precisa se adaptar e adotar medidas que visem a redução dessas emissões, para mitigar os efeitos do aquecimento global e atender a padrões ambientais e de sustentabilidade.

### Solução Proposta
A solução desenvolvida é um sistema integrado de monitoramento e recomendação que visa otimizar as práticas agrícolas, considerando a emissão de carbono gerada por cada ação. O sistema é dividido em quatro camadas:

## 📁 Estrutura de pastas

Dentre os arquivos e pastas presentes na raiz do projeto, definem-se:

- <b>assets</b>: aqui estão os arquivos relacionados a elementos não-estruturados deste repositório, como imagens.

- <b>document</b>: aqui estão todos os documentos do projeto.

- <b>src</b>: Todo o código fonte criado para o desenvolvimento do projeto

- <b>README.md</b>: arquivo que serve como guia e explicação geral sobre o projeto (o mesmo que você está lendo agora).

## 🔧 Como executar o código

Requer a instalação do Python3, R e sistema operacional Linux.

Nota: se estiver usando MS Windows, use WSL2 e instale a distro Linux de sua
escolha.

1. Clone o repositório git -- o `depth=1` é usado para aquele que optar por
   obter somente o commit atual ao invés de todos os commits (histórico) do 
   repositório.

        $ git clone https://github.com/RevoluxAI/fase2-cap6.git --depth=1

2. Para evitar instalar bibliotecas extras em sua máquina, use Python Virtual
   Environment (python3-venv):

        $ cd fase2-cap6
        $ python3 -m venv cap6-venv 
        $ . cap6-venv/bin/activate

**Nota:** se não tiver o módulo `venv`, instale usando a sua mirror; o nome
do pacote, provavelmente é: `python3-venv`.

use o "activate" consoante ao seu terminal em uso. Por exemplo:
se estiver usando Bash Shell, execute:

        $ . cap6-venv/bin/activate

para mais detalhes, consulte a documentação do python venv. :-)

Se a execução deu certo, o seu terminal ficará parecido com isto:

        (cap1-venv) $

Agora, basta atualizar o python3-pip e instalar as bibliotecas do Python3.
Use o `requirements.txt`:

        (cap1-venv) $ python3 -m pip install --upgrade pip
        (cap1-venv) $ python3 -m pip install -r requirements.txt

**Nota:** Caso não tenha o "PIP", instale. O pacote deve constar na mirror
com o nome: `python3-pip`

### Execução

O projeto pode ser executado por linha de comando usando o CLI:

    $ python3 main.py

## 🗃 Histórico de lançamentos

* 0.1.0 - 22/04/2025
    * 
* 0.0.2 - 21/04/2025
    * 
* 0.0.1 - 12/04/2025
    *

## 📋 Licença

<img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/cc.svg?ref=chooser-v1"><img style="height:22px!important;margin-left:3px;vertical-align:text-bottom;" src="https://mirrors.creativecommons.org/presskit/icons/by.svg?ref=chooser-v1"><p xmlns:cc="http://creativecommons.org/ns#" xmlns:dct="http://purl.org/dc/terms/"><a property="dct:title" rel="cc:attributionURL" href="https://github.com/agodoi/template">MODELO GIT FIAP</a> por <a rel="cc:attributionURL dct:creator" property="cc:attributionName" href="https://fiap.com.br">Fiap</a> está licenciado sobre <a href="http://creativecommons.org/licenses/by/4.0/?ref=chooser-v1" target="_blank" rel="license noopener noreferrer" style="display:inline-block;">Attribution 4.0 International</a>.</p>
