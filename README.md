🪙 Crypto Data Pipeline: Medallion Architecture
Este projeto implementa um pipeline de dados robusto para o monitoramento de criptoativos, seguindo a Arquitetura Medalhão (Bronze, Silver, Gold). O objetivo é transformar dados brutos de mercado em indicadores financeiros prontos para análise, com foco em conversão monetária e segmentação de mercado.

🏗️ Arquitetura do Projeto
O fluxo de dados foi desenhado para garantir a qualidade e o refinamento progressivo da informação:

Bronze (Raw): Extração direta da API e armazenamento do JSON bruto.

Silver (Cleaned): Limpeza, tipagem de dados e histórico de processamento.

Gold (Curated): Camada de negócio com conversão BRL, Rankings e Tiers.

🛠️ Tecnologias e Ferramentas
Linguagem: Python 3.9+

Bibliotecas: Pandas, NumPy, yFinance

Banco de Dados: PostgreSQL

ORM/Interface: SQLAlchemy 2.0

Observabilidade: Logging (Padrão Python)

🚀 Funcionalidades da Camada Gold
A camada final (Gold) entrega valor analítico através das seguintes implementações técnicas:

Conversão em Tempo Real: Integração com a API do Yahoo Finance para converter todas as métricas de USD para BRL.

Segmentação de Mercado (Tiering): Classificação automática em Large Cap, Medium Cap e Low Cap baseada no Market Cap.

Ranking de Ativos: Cálculo de posição de mercado (market_rank) utilizando lógica de ranking do Pandas.

Integridade de Dados: Aplicação de máscaras de performance para tratar variações negativas e garantir KPIs consistentes.

Persistência Eficiente: Uso de TRUNCATE para atualização de lote e ON CONFLICT para garantir a unicidade do coin_id.

📝 Exemplo de Observabilidade (Logs)
O pipeline é totalmente monitorado por logs em português, facilitando o rastreio de erros e auditoria do processo:

Plaintext
2026-04-07 08:00:01 - INFO - Iniciando o processo de ETL para a camada Gold...
2026-04-07 08:00:03 - INFO - Tabela Silver lida com sucesso! 100 registros extraídos.
2026-04-07 08:00:04 - INFO - Cotação Atual (USDBRL=X) obtida com sucesso: R$ 5.1234
2026-04-07 08:00:07 - INFO - Sucesso: 100 registros inseridos/atualizados na Gold.
📁 Estrutura de Arquivos
Plaintext
├── connection.py        # Parâmetros de conexão com o PostgreSQL
├── ingestion_bronze.py  # Coleta de dados brutos (API)
├── ingestion_silver.py  # Limpeza e padronização (Silver)
├── ingestion_gold.py    # Regras de negócio e enriquecimento (Gold)
└── requirements.txt     # Dependências do projeto
⚙️ Como Executar
Clone o repositório:

Bash
git clone https://github.com/seu-usuario/crypto-data-pipeline.git
Instale as dependências:

Bash
pip install -r requirements.txt
Configure suas credenciais no arquivo connection.py.

Execute o pipeline:

Bash
python ingestion_gold.py
Desenvolvido por Lúcio - Data Professional | BI & Data Engineering