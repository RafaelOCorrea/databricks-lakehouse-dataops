# 📊 Enterprise Lakehouse Platform: Medallion Architecture & DataOps with Databricks

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-00A4E4)
![Delta Live Tables](https://img.shields.io/badge/DLT-Data_Quality-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![DataOps CI/CD](https://img.shields.io/badge/DataOps-GitHub_Actions_%2B_DABs-2088FF?logo=githubactions&logoColor=white)

Este projeto implementa uma solução completa de **Engenharia de Dados em Nuvem (Lakehouse)** utilizando o **Databricks**, seguindo a arquitetura medalhão (**Bronze -> Silver -> Gold**), governança centralizada no **Unity Catalog**, validação automatizada de qualidade com **Delta Live Tables (DLT Expectations)** e esteira de **CI/CD / DataOps** via **Databricks Asset Bundles (DABs)**.

---

## 🏗️ Arquitetura da Solução

```mermaid
flowchart TD
    RAW[Volume Unity Catalog - df_sales.csv]
    B1[Bronze - sales_raw com Auditoria]
    V_CLEAN[Silver View - Limpeza e Deduplicacao]
    D_PROD[Silver - dim_produto]
    D_CLI[Silver - dim_cliente]
    D_LOC[Silver - dim_localizacao]
    D_ENV[Silver - dim_modo_envio]
    F_VENDAS[Silver - ft_vendas Fato]
    G_CAT[Gold - vendas_por_categoria]
    G_REG[Gold - desempenho_regional]
    G_CLI[Gold - metrica_clientes]
    BI[Dashboards e Analytics]

    RAW --> B1
    B1 --> V_CLEAN
    V_CLEAN --> D_PROD
    V_CLEAN --> D_CLI
    V_CLEAN --> D_LOC
    V_CLEAN --> D_ENV
    V_CLEAN --> F_VENDAS
    F_VENDAS --> G_CAT
    F_VENDAS --> G_REG
    F_VENDAS --> G_CLI
    D_PROD --> G_CAT
    D_LOC --> G_REG
    D_CLI --> G_CLI
    G_CAT --> BI
    G_REG --> BI
    G_CLI --> BI
```

---

## 🛡️ Governança & Data Quality (DLT Expectations)

Para garantir confiabilidade aos times de negócio e analistas, o pipeline possui regras ativas de qualidade de dados integradas ao fluxo de execução:

| Camada | Tabela | Regra de Qualidade | Tipo | Objetivo de Negócio |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | `sales_raw` | `valid_primary_keys` | `@dlt.expect_or_drop` | Descartar registros sem `order_id` ou `product_id`. |
| **Bronze** | `sales_raw` | `valid_source_file_metadata` | `@dlt.expect` | Monitorar metadados de rastreabilidade de origem. |
| **Silver** | `ft_vendas` | `valid_positive_sales` | `@dlt.expect_or_drop` | Garantir que o valor das vendas seja estritamente positivo. |
| **Silver** | `ft_vendas` | `valid_positive_quantity` | `@dlt.expect_or_drop` | Filtrar quantidades menores ou iguais a zero. |
| **Silver** | `ft_vendas` | `valid_discount_range` | `@dlt.expect` | Monitorar descontos fora da faixa padrão (0 a 100%). |
| **Silver** | `ft_vendas` | `valid_dates_chronology` | `@dlt.expect` | Validar se a data de envio é posterior ou igual à data do pedido. |
| **Silver** | Dimensões | `valid_keys` | `@dlt.expect_or_drop` | Preservar integridade referencial nas dimensões. |
| **Gold** | Data Marts | `valid_revenue` | `@dlt.expect` | Assegurar integridade dos KPIs agregados de receita. |

### 📈 Resultados Reais de Qualidade de Dados:
* **Taxa de Conformidade:** **99,4%** (50.951 registros aprovados e materializados)
* **Anomalias Tratadas:** **0,6%** (301 registros inválidos/corrompidos descartados automaticamente na borda)

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

```mermaid
flowchart LR
    DEV[Feature Branch ou PR] --> GHA_VAL[GitHub Actions - Validate]
    GHA_VAL --> MERGE[Merge para main]
    MERGE --> GHA_DEP[GitHub Actions - Deploy Prod]
    GHA_DEP --> DBX[Databricks - Pipeline Atualizado]
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

## 🛠️ Tecnologias e Ferramentas

* **Compute & Storage:** Databricks Serverless, Photon Engine, Apache Spark (PySpark), Delta Lake
* **Governança & Catálogo:** Unity Catalog (Catalogs, Schemas, Volumes, Managed Tables, Materialized Views)
* **Orquestração & Pipeline:** Delta Live Tables (DLT / Lakeflow)
* **DataOps & CI/CD:** Databricks Asset Bundles (DABs), GitHub Actions, Git
