# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Medalhão: Portfolio Lakehouse (Bronze -> Silver -> Gold)
# MAGIC Este notebook define as tabelas e views declarativas usando Delta Live Tables (DLT) no Unity Catalog com **Data Quality Expectations**.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col,
    round,
    regexp_replace,
    current_timestamp,
    expr,
    sum as _sum,
    avg,
    countDistinct
)

# -------------------------------------------------------------------------
# 1. CAMADA BRONZE (Leitura do Volume e Ingestão com Metadados)
# -------------------------------------------------------------------------
@dlt.table(
    name="portfolio_lakehouse.bronze.sales_raw",
    comment="Dados brutos de vendas ingeridos a partir do Volume Unity Catalog com auditoria"
)
@dlt.expect_or_drop("valid_primary_keys", "order_id IS NOT NULL AND product_id IS NOT NULL")
@dlt.expect("valid_source_file_metadata", "_source_file IS NOT NULL")
def sales_raw():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load("/Volumes/portfolio_lakehouse/raw/raw_data/df_sales.csv")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", expr("_metadata.file_path"))
    )

# -------------------------------------------------------------------------
# 2. CAMADA SILVER (Limpeza, Deduplicação e Modelagem Star Schema)
# -------------------------------------------------------------------------
@dlt.view(
    name="sales_cleaned_view",
    comment="Visão temporária de limpeza, tratamento de caracteres e tipagem"
)
def sales_cleaned_view():
    return (
        dlt.read("portfolio_lakehouse.bronze.sales_raw")
        .dropDuplicates(["order_id", "product_id"])
        .withColumn("sales_clean", regexp_replace(col("sales"), r'[\"\s]', ''))
        .withColumn("discount_clean", regexp_replace(col("discount"), r'[\"\s]', ''))
        .withColumn("margem_clean", regexp_replace(col("margem_lucro"), r'[\"\s]', ''))
        .withColumn("quantity_clean", regexp_replace(col("quantity"), r'[\"\s]', ''))
        .withColumn("discount_clean", regexp_replace(col("discount_clean"), ",", "."))
        .withColumn("margem_clean", regexp_replace(col("margem_clean"), ",", "."))
        .withColumn("sales", expr("try_cast(sales_clean as double)").cast("integer"))
        .withColumn("quantity", expr("try_cast(quantity_clean as double)").cast("integer"))
        .withColumn("discount", expr("try_cast(discount_clean as double)"))
        .withColumn("margem_lucro", round(expr("try_cast(margem_clean as double)"), 4))
        .withColumn("profit", round(expr("try_cast(profit as double)"), 2))
        .withColumn("shipping_cost", round(expr("try_cast(shipping_cost as double)"), 2))
        .drop("sales_clean", "discount_clean", "margem_clean", "quantity_clean")
        .withColumn("_updated_at", current_timestamp())
    )

@dlt.table(
    name="portfolio_lakehouse.silver.dim_produto",
    comment="Dimensão Produto normalizada"
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("valid_product_name", "product_name IS NOT NULL")
def dim_produto():
    return dlt.read("sales_cleaned_view").select("product_id", "product_name", "category", "sub_category").distinct()

@dlt.table(
    name="portfolio_lakehouse.silver.dim_cliente",
    comment="Dimensão Cliente normalizada"
)
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL")
@dlt.expect("valid_segment", "segment IS NOT NULL")
def dim_cliente():
    return dlt.read("sales_cleaned_view").select("customer_name", "segment").distinct()

@dlt.table(
    name="portfolio_lakehouse.silver.dim_localizacao",
    comment="Dimensão Localização normalizada"
)
@dlt.expect_or_drop("valid_location", "country IS NOT NULL AND state IS NOT NULL")
def dim_localizacao():
    return dlt.read("sales_cleaned_view").select("country", "state", "region", "market").distinct()

@dlt.table(
    name="portfolio_lakehouse.silver.dim_modo_envio",
    comment="Dimensão Modo de Envio e Prioridade"
)
@dlt.expect_or_drop("valid_ship_mode", "ship_mode IS NOT NULL")
def dim_modo_envio():
    return dlt.read("sales_cleaned_view").select("ship_mode", "order_priority").distinct()

@dlt.table(
    name="portfolio_lakehouse.silver.ft_vendas",
    comment="Tabela Fato Vendas com regras rigorosas de qualidade de dados"
)
@dlt.expect_or_drop("valid_positive_sales", "sales > 0")
@dlt.expect_or_drop("valid_positive_quantity", "quantity > 0")
@dlt.expect("valid_discount_range", "discount >= 0 AND discount <= 1.0")
@dlt.expect("valid_shipping_cost", "shipping_cost >= 0")
@dlt.expect("valid_dates_chronology", "order_date <= ship_date")
def ft_vendas():
    return dlt.read("sales_cleaned_view").select(
        "order_id",
        "order_date",
        "ship_date",
        "customer_name",
        "product_id",
        "country",
        "state",
        "ship_mode",
        "sales",
        "quantity",
        "discount",
        "profit",
        "shipping_cost",
        "margem_lucro",
        "_updated_at"
    )

# -------------------------------------------------------------------------
# 3. CAMADA GOLD (Data Marts Analíticos com Validações de Métricas de Negócio)
# -------------------------------------------------------------------------
@dlt.table(
    name="portfolio_lakehouse.gold.vendas_por_categoria",
    comment="KPIs agregados por Categoria e Subcategoria"
)
@dlt.expect("valid_category_revenue", "faturamento_total > 0")
@dlt.expect("valid_category_items", "qtd_itens_vendidos > 0")
def vendas_por_categoria():
    ft = dlt.read("portfolio_lakehouse.silver.ft_vendas")
    dim_prod = dlt.read("portfolio_lakehouse.silver.dim_produto")
    return (
        ft.join(dim_prod, "product_id", "inner")
        .groupBy("category", "sub_category")
        .agg(
            _sum("sales").alias("faturamento_total"),
            _sum("profit").alias("lucro_total"),
            _sum("quantity").alias("qtd_itens_vendidos"),
            round(avg("margem_lucro"), 4).alias("margem_media")
        )
        .orderBy(col("faturamento_total").desc())
    )

@dlt.table(
    name="portfolio_lakehouse.gold.desempenho_regional",
    comment="KPIs agregados por Mercado, Região e País"
)
@dlt.expect("valid_regional_revenue", "faturamento_total > 0")
@dlt.expect("valid_orders_count", "total_pedidos > 0")
def desempenho_regional():
    ft = dlt.read("portfolio_lakehouse.silver.ft_vendas")
    dim_loc = dlt.read("portfolio_lakehouse.silver.dim_localizacao")
    return (
        ft.join(dim_loc, ["country", "state"], "inner")
        .groupBy("market", "region", "country")
        .agg(
            _sum("sales").alias("faturamento_total"),
            _sum("profit").alias("lucro_total"),
            _sum("shipping_cost").alias("custo_frete_total"),
            countDistinct("order_id").alias("total_pedidos")
        )
        .orderBy(col("faturamento_total").desc())
    )

@dlt.table(
    name="portfolio_lakehouse.gold.metrica_clientes",
    comment="KPIs de Clientes e Segmentos"
)
@dlt.expect("valid_segment_revenue", "faturamento_total > 0")
@dlt.expect("valid_ticket_medio", "ticket_medio_por_pedido > 0")
def metrica_clientes():
    ft = dlt.read("portfolio_lakehouse.silver.ft_vendas")
    dim_cli = dlt.read("portfolio_lakehouse.silver.dim_cliente")
    return (
        ft.join(dim_cli, "customer_name", "inner")
        .groupBy("segment")
        .agg(
            countDistinct("customer_name").alias("total_clientes"),
            _sum("sales").alias("faturamento_total"),
            _sum("profit").alias("lucro_total"),
            round(_sum("sales") / countDistinct("order_id"), 2).alias("ticket_medio_por_pedido")
        )
        .orderBy(col("faturamento_total").desc())
    )
