# 📊 Enterprise Lakehouse Platform: Medallion Architecture & DataOps with Databricks

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-00A4E4)
![Delta Live Tables](https://img.shields.io/badge/DLT-Data_Quality-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![DataOps CI/CD](https://img.shields.io/badge/DataOps-GitHub_Actions_%2B_DABs-2088FF?logo=githubactions&logoColor=white)
![XGBoost](https://img.shields.io/badge/ML-XGBoost_%2B_Scikit--Learn-orange?logo=python&logoColor=white)

Este projeto implementa uma solução completa de **Engenharia de Dados em Nuvem (Lakehouse)** utilizando o **Databricks**, seguindo a arquitetura medalhão (**Bronze -> Silver -> Gold**), governança centralizada no **Unity Catalog**, validação automatizada de qualidade com **Delta Live Tables (DLT Expectations)** e esteira de **CI/CD / DataOps** via **Databricks Asset Bundles (DABs)**.

---

## 🏗️ Arquitetura da Solução

```text
+-------------------------------------------------------------------------------+
|                      0. INGESTÃO & STORAGE BRUTO                              |
|              Volume Unity Catalog: /Volumes/.../df_sales.csv                  |
+-------------------------------------------------------------------------------+
                                      │
                                      ▼
+-------------------------------------------------------------------------------+
|                       🥉 CAMADA BRONZE (RAW INGESTION)                        |
|            sales_raw (Materialized View + Metadados de Auditoria)             |
|            * Preservação integral 'As-Is' (100% dos dados gravados)           |
+-------------------------------------------------------------------------------+
                                      │
                                      ▼
+-------------------------------------------------------------------------------+
|                 🥈 CAMADA SILVER (LIMPEZA, DQ & STAR SCHEMA)                  |
|       Visão de Preparo: sales_cleaned_view (Deduplicação & Tipagem Segura)    |
|                                                                               |
|   [dim_produto]      [dim_cliente]      [dim_localizacao]     [dim_modo_envio]|
|         │                  │                    │                     │       |
|         └──────────────────┴─────────┬──────────┴─────────────────────┘       |
|                                      │                                        |
|                       [ft_vendas (Fato com Data Quality)]                     |
+-------------------------------------------------------------------------------+
                                      │
                                      ▼
+-------------------------------------------------------------------------------+
|                    🥇 CAMADA GOLD (DATA MARTS ANALÍTICOS)                     |
|                                                                               |
|  [vendas_por_categoria]    [desempenho_regional]      [metrica_clientes]      |
|  (KPIs por Categoria)      (KPIs por Mercado/Região)  (Ticket Médio & Clientes|
+-------------------------------------------------------------------------------+
                                      │
                                      ▼
+-------------------------------------------------------------------------------+
|                            📈 CAMADA DE CONSUMO                               |
|                     Databricks Lakeview Dashboards / BI                       |
+-------------------------------------------------------------------------------+
```

---

## 🛡️ Governança & Data Quality (DLT Expectations)

Seguindo as melhores práticas da Arquitetura Medalhão:
* **Camada Bronze:** Cópia fiel e imutável (*As-Is*) para garantir **replayability e auditoria**. Nenhum registro é descartado.
* **Camada Silver:** Aplicação rigorosa de **Data Quality (`@dlt.expect_or_drop`)**, filtrando anomalias antes do consumo analítico.

| Camada | Tabela | Regra de Qualidade | Tipo | Objetivo de Negócio |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `sales_raw` | `valid_source_file_metadata` | `@dlt.expect` | Monitorar metadados de rastreabilidade e carga integral sem descarte. |
| **Silver** | `ft_vendas` | `valid_primary_keys` | `@dlt.expect_or_drop` | Descartar registros sem `order_id` ou `product_id`. |
| **Silver** | `ft_vendas` | `valid_positive_sales` | `@dlt.expect_or_drop` | Garantir que o valor das vendas seja estritamente positivo. |
| **Silver** | `ft_vendas` | `valid_positive_quantity` | `@dlt.expect_or_drop` | Filtrar quantidades menores ou iguais a zero. |
| **Silver** | `ft_vendas` | `valid_discount_range` | `@dlt.expect` | Monitorar descontos fora da faixa padrão (0 a 100%). |
| **Silver** | `ft_vendas` | `valid_dates_chronology` | `@dlt.expect` | Validar se a data de envio é posterior ou igual à data do pedido. |
| **Silver** | Dimensões | `valid_keys` | `@dlt.expect_or_drop` | Preservar integridade referencial nas dimensões. |
| **Gold** | Data Marts | `valid_revenue` | `@dlt.expect` | Assegurar integridade dos KPIs agregados de receita. |

### 📈 Resultados Reais de Qualidade de Dados na Camada Silver:
* **Taxa de Conformidade:** **99,4%** (50.951 registros limpos e materializados)
* **Anomalias Tratadas:** **0,6%** (301 registros inválidos/corrompidos descartados automaticamente na borda Silver)

---

## 📦 Modelagem de Dados

### 1. Camada Silver (Modelagem Dimensional Star Schema)
* **`dim_produto`**: `product_id`, `product_name`, `category`, `sub_category`
* **`dim_cliente`**: `customer_name`, `segment`
* **`dim_localizacao`**: `country`, `state`, `region`, `market`
* **`dim_modo_envio`**: `ship_mode`, `order_priority`
* **`ft_vendas`**: Fato com chaves dimensionais e métricas (`sales`, `quantity`, `discount`, `profit`, `shipping_cost`, `margem_lucro`, `_updated_at`).

### 2. Camada Gold (Data Marts de Negócio)
* **`vendas_por_categoria`**: Agregação de faturamento, margem média e volume por categoria e subcategoria de produto.
* **`desempenho_regional`**: Análise geográfica de lucratividade, volume de vendas e custos logísticos por país e região.
* **`metrica_clientes`**: Indicadores de valor do cliente, faturamento por segmento e ticket médio por pedido.

---

## 🤖 DataOps: CI/CD com Databricks Asset Bundles (DABs)

O projeto é gerenciado como **Infraestrutura como Código (IaC)**, permitindo deploys automatizados entre ambientes de Desenvolvimento e Produção:

```text
  [💻 Feature Branch / PR]
             │
             ▼
  [🔍 GitHub Actions: Validate] (databricks bundle validate -t dev)
             │
             ▼ (Merge para main)
  [🚀 GitHub Actions: Deploy Prod] (databricks bundle deploy -t prod)
             │
             ▼
  [☁️ Databricks Lakehouse: Pipeline Atualizado]
```

* **`databricks.yml`**: Configuração central do bundle com definição de targets (`dev`, `prod`).
* **`resources/pipeline.yml`**: Especificação declarativa do pipeline DLT (Serverless + Photon).
* **`.github/workflows/deploy.yml`**: Workflow de automação que valida os bundles em PRs e faz deploy automático na branch `main`.

---

## 💻 Como Executar Localmente

### Pré-requisitos
* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) instalado e configurado
* Python 3.10+
* Acesso a um workspace Databricks com Unity Catalog ativado

### 1. Clonar o Repositório
```bash
git clone https://github.com/RafaelOCorrea/databricks-lakehouse-dataops.git
cd databricks-lakehouse-dataops
```

### 2. Validar as Definições do Bundle
```bash
databricks bundle validate -t dev
```

### 3. Fazer o Deploy para o Databricks
```bash
databricks bundle deploy -t dev
```

### 4. Executar o Pipeline
```bash
databricks bundle run portfolio_lakehouse_pipeline -t dev
```

---

## 🤖 Machine Learning: Modelo de Propensão de Compra (Purchase Propensity Score)

Como extensão analítica do pipeline Lakehouse, este projeto implementa um modelo de **Machine Learning supervisionado** para estimar a **probabilidade de recompra de cada cliente** (score de 0 a 1), integrando diretamente com os dados governados no Unity Catalog.

### Arquitetura do Modelo

```text
[Silver: ft_vendas + dim_cliente + dim_localizacao]
                      |
                      v
        +-----------------------------+
        | Feature Engineering (RFV)  |
        |  Recencia, Frequencia,      |
        |  Valor, Ticket Medio,       |
        |  Margem, Diversidade        |
        +-----------------------------+
                      |
                      v
        +-----------------------------+
        | Estrategia Multi-Janela     |  <- Evita Data Leakage
        | 4 Cutoffs (2012-2013)       |
        | Target: comprou nos         |
        | proximos 90 dias? (0/1)     |
        +-----------------------------+
                      |
                      v
        +-----------------------------+
        | Pipeline Scikit-Learn       |
        | StandardScaler (numericas)  |
        | OneHotEncoder (categoricas) |
        | XGBClassifier               |
        |  n_estimators=150           |
        |  max_depth=4                |
        |  learning_rate=0.05         |
        +-----------------------------+
                      |
                      v
        +-----------------------------+
        | Scoring: 795 clientes       |
        | Ordenados por score 0 a 1   |
        | Faixas de acao de CRM       |
        +-----------------------------+
                      |
                      v
  [Gold: dm_propensao_compra_clientes]
```

### Features Utilizadas no Modelo (Datamart de Clientes)

| Feature | Descricao |
| :--- | :--- |
| `recencia_dias` | Dias desde a ultima compra do cliente |
| `frequencia_pedidos` | Numero de pedidos unicos realizados |
| `valor_total_gasto` | Faturamento total acumulado pelo cliente |
| `ticket_medio` | Valor medio por pedido |
| `lucro_total` | Lucro total gerado pelo cliente |
| `desconto_medio` | Percentual medio de desconto recebido |
| `margem_media` | Margem de lucro media das compras |
| `qtd_categorias_distintas` | Diversidade de categorias de produto compradas |
| `tempo_relacionamento_dias` | Tenure: dias entre primeira e ultima compra |
| `segment` | Segmento do cliente (Consumer / Corporate / Home Office) |
| `market` | Mercado geografico predominante (LATAM, US, EU, APAC...) |

### Estrategia de Validacao Temporal (Anti-Data Leakage)

O modelo foi treinado com uma **estrategia multi-janela** para evitar data leakage e capturar sazonalidade real:

| Cutoff | Clientes | Compraram nos 90 dias seguintes | Taxa |
| :--- | :--- | :--- | :--- |
| 2012-06-30 | 795 | 678 | 85,3% |
| 2012-12-31 | 795 | 580 | 73,0% |
| 2013-06-30 | 795 | 725 | 91,2% |
| 2013-12-31 | 795 | 653 | 82,1% |
| **Total (treino agregado)** | **3.180 amostras** | **2.636 positivos** | **82,9%** |

### Resultados de Performance do Modelo

```
============================================================
 AVALIACAO DE PERFORMANCE NO CONJUNTO DE TESTE (HOLDOUT 20%)
============================================================
  ROC-AUC Score : 0.5083
  Brier Score   : 0.2207

  Relatorio de Classificacao:
                   precision    recall  f1-score   support
  Nao Comprou (0)      0.17      0.28      0.22       109
      Comprou (1)      0.83      0.72      0.77       527
         accuracy                          0.65       636

  Matriz de Confusao:
  Verdadeiros Negativos :  31 | Falsos Positivos : 78
  Falsos Negativos      : 147 | Verdadeiros Positivos: 380
============================================================
```

> **Nota tecnica:** O ROC-AUC proximo de 0,50 reflete uma caracteristica da propria base: clientes B2B recorrentes com frequencia media de 25 pedidos por cliente compram em praticamente qualquer janela de 90 dias (82,9% de positivos). Esse e um cenario classico de **Target Imbalance por Alta Fidelizacao**. Em producao, a solucao seria refinar a definicao de churn (ex.: inatividade superior a 180 dias) ou trabalhar com score relativo de aceleracao de compra.

### Ranking de Propensao de Compra — Top 10 Clientes

| # | Cliente | Score | Recencia | Pedidos | Ticket Medio | Segmento |
| :---: | :--- | :---: | :---: | :---: | :---: | :--- |
| 1 | Sharelle Roach | **0.8479** | 8 dias | 24 | R$ 541,88 | Home Office |
| 2 | Sibella Parks | **0.8341** | 16 dias | 24 | R$ 300,88 | Corporate |
| 3 | Robert Barroso | **0.8297** | 150 dias | 25 | R$ 250,24 | Corporate |
| 4 | Vivian Mathis | **0.8218** | 8 dias | 23 | R$ 169,17 | Consumer |
| 5 | Corinna Mitchell | **0.8127** | 27 dias | 30 | R$ 519,33 | Home Office |
| 6 | David Bremer | **0.8049** | 5 dias | 20 | R$ 313,75 | Corporate |
| 7 | Candace McMahon | **0.7974** | 40 dias | 28 | R$ 528,50 | Corporate |
| 8 | Roland Fjeld | **0.7851** | 1 dia | 26 | R$ 503,46 | Consumer |
| 9 | Sean Braxton | **0.7829** | 41 dias | 27 | R$ 759,78 | Corporate |
| 10 | Roland Murray | **0.7779** | 16 dias | 26 | R$ 284,73 | Consumer |

### Distribuicao Estrategica da Base (795 clientes)

| Faixa de Propensao | Clientes | % da Base | Acao de CRM Recomendada |
| :--- | :---: | :---: | :--- |
| **Alta Propensao (score >= 0.70)** | 33 | 4,2% | Oferta direta / Upsell imediato |
| **Media Propensao (0.40 a 0.70)** | 637 | 80,1% | Nutricao / Campanhas de engajamento |
| **Baixa Propensao (score < 0.40)** | 125 | 15,7% | Reativacao / Win-back / Pesquisa de churn |

### Arquivos do Modelo

| Arquivo | Descricao |
| :--- | :--- |
| [`ml_propensao_compra.py`](ml_propensao_compra.py) | Script Python standalone: executa localmente com `py ml_propensao_compra.py` |
| [`notebook_propensao_compra.py`](notebook_propensao_compra.py) | Notebook Databricks: le direto das tabelas Silver do Unity Catalog e grava na Gold |
| [`dm_propensao_compra_clientes.csv`](dm_propensao_compra_clientes.csv) | Output: ranking completo dos 795 clientes com score, decil e faixa de CRM |

---

## 🛠️ Tecnologias e Ferramentas

* **Compute & Storage:** Databricks Serverless, Photon Engine, Apache Spark (PySpark), Delta Lake
* **Governanca & Catalogo:** Unity Catalog (Catalogs, Schemas, Volumes, Managed Tables, Materialized Views)
* **Orquestracao & Pipeline:** Delta Live Tables (DLT / Lakeflow)
* **DataOps & CI/CD:** Databricks Asset Bundles (DABs), GitHub Actions, Git
* **Machine Learning:** XGBoost, Scikit-Learn (Pipeline, StandardScaler, OneHotEncoder, StratifiedKFold), Pandas, NumPy
