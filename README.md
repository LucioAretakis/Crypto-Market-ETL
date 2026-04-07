Markdown
# 🪙 Crypto Data Pipeline: Medallion Architecture

Este projeto implementa um pipeline de dados robusto para o monitoramento de criptoativos, utilizando a **Arquitetura Medalhão (Bronze, Silver, Gold)**. O objetivo é transformar dados brutos extraídos via API em indicadores financeiros refinados, prontos para consumo em ferramentas de Business Intelligence (BI).

## 🏗️ Arquitetura do Projeto

O fluxo de dados foi desenhado para garantir a qualidade, integridade e o refinamento progressivo da informação:

1.  **Bronze (Raw):** Camada de ingestão inicial. Extração direta da API e armazenamento dos dados brutos.
2.  **Silver (Cleaned):** Processo de limpeza, tipagem de dados e seleção do lote mais recente utilizando Window Functions (`ROW_NUMBER`) para garantir a unicidade por `coin_id`.
3.  **Gold (Curated):** Camada final de negócio. Aplicação de enriquecimento com dados externos (yFinance), conversão de moeda (BRL), rankings e categorização de mercado.



---

## 🛠️ Tecnologias e Ferramentas

* **Linguagem:** Python 3.9+
* **Manipulação de Dados:** Pandas & NumPy
* **Integração Financeira:** yFinance API (Cotação USD/BRL em tempo real)
* **Banco de Dados:** PostgreSQL
* **Interface de Banco:** SQLAlchemy 2.0 (Uso de `engine.begin` e `text`)
* **Observabilidade:** Logging (Mensagens estruturadas em Português)

---

## 🚀 Funcionalidades da Camada Gold

A camada **Gold** entrega o valor final do pipeline através das seguintes implementações:

* **Conversão de Moeda Dinâmica:** Integração com o Yahoo Finance para converter preços e variações de USD para BRL no momento da carga.
* **Segmentação de Mercado (Tiering):** Classificação automática dos ativos em *Large Cap*, *Medium Cap* e *Low Cap* via lógica vetorizada.
* **Ranking de Ativos:** Cálculo de posição de mercado (`market_rank`) baseado no valor de capitalização.
* **Tratamento de Performance:** Aplicação de máscaras de dados para tratar variações negativas e garantir a consistência dos KPIs.
* **Estratégia de Carga:** Uso de `TRUNCATE` para atualização total do lote atual e `ON CONFLICT (UPSERT)` para garantir a integridade da Primary Key.

---

## 📝 Exemplo de Observabilidade (Logs)

O pipeline utiliza o módulo `logging` para monitorar cada etapa do processo em tempo real:

```text
2026-04-07 08:00:01 - INFO - Iniciando o processo de ETL para a camada Gold...
2026-04-07 08:00:02 - INFO - Conexão com o banco de dados estabelecida com sucesso.
2026-04-07 08:00:03 - INFO - Tabela Silver lida com sucesso! 100 registros extraídos.
2026-04-07 08:00:04 - INFO - Cotação Atual (USDBRL=X) obtida com sucesso: R$ 5.1234
2026-04-07 08:00:05 - INFO - Aplicando regra de negócio: Categorização de Market Tier...
2026-04-07 08:00:07 - INFO - Sucesso: 100 registros inseridos/atualizados na Gold.
```

## 📁 Estrutura de Arquivos

Plaintext
.
├── connection.py        # Configuração da Engine SQLAlchemy e conexão
├── ingestion_bronze.py  # Script de extração da API (Camada Bronze)
├── ingestion_silver.py  # Script de limpeza e padronização (Camada Silver)
├── ingestion_gold.py    # Script de regras de negócio e carga final (Camada Gold)
└── requirements.txt     # Dependências do projeto


## ⚙️ Como Executar

1. Clonar o Repositório
Bash
git clone [https://github.com/seu-usuario/crypto-data-pipeline.git](https://github.com/seu-usuario/crypto-data-pipeline.git)
cd crypto-data-pipeline
2. Instalar Dependências
Bash
pip install -r requirements.txt
3. Configurar Conexão
Edite o arquivo connection.py com suas credenciais do PostgreSQL:

Host, Usuário, Senha e Porta.

4. Rodar o Pipeline
Para processar os dados da camada final:

Bash
python ingestion_gold.py
Desenvolvido por Lucio - Data Analysis| Data Engineering