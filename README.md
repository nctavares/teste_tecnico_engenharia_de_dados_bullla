## Teste Técnico de Engenharia de Dados - Bullla

Case de entrevista para vaga de Eng na Bullla

Solução para o desafio técnico de engenharia de dados, implementando um pipeline de duas fases para extração, tratamento e armazenamento de dados de clientes.

Tecnologias: Python, Pandas, SQLAlchemy, Apache Beam, Git, SQLite.

Como Executar o Projeto
Pré-requisitos: Git e Python instalados.


# Crie e Ative o Ambiente Virtual:


python -m venv venv
.\venv\Scripts\activate


# Instale as Dependências:

pip install -r requirements.txt


# Estrutura da Solução

Fase 1: Os dados brutos são carregados em um banco de dados SQLite e consultados via Jupyter Notebook para gerar arquivos CSV com listas de clientes segmentados.

Fase 2: Uma pipeline ETL em Python com Apache Beam processa os arquivos CSV, aplicando limpeza, enriquecimento, remoção de duplicados e salvando os resultados finais em formato Parquet.


