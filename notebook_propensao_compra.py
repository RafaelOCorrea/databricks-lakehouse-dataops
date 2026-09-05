# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Modelo de Propensão de Compra (Purchase Propensity Score)
# MAGIC ### Arquitetura Lakehouse + Scikit-Learn & XGBoost
# MAGIC 
# MAGIC Este notebook:
# MAGIC 1. Constrói o **Datamart de Clientes (Feature Store)** com métricas de **RFV (Recência, Frequência e Valor)** a partir das tabelas Silver do Unity Catalog.
# MAGIC 2. Aplica **Time-Based Splitting** com janela histórica de observação e período futuro de predição (evitando *data leakage*).
# MAGIC 3. Treina um classificador **XGBoost** calibrado para estimar a probabilidade de recompra.
# MAGIC 4. Avalia as métricas de performance (**ROC-AUC, Precision, Recall, Matriz de Confusão**).
# MAGIC 5. Realiza o **Scoring (0 a 1)** para todos os clientes ordenados do maior para o menor.
# MAGIC 6. Grava a tabela final ranqueada na camada Gold do Unity Catalog: `portfolio_lakehouse.gold.dm_propensao_compra_clientes`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importação de Bibliotecas

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime

# Scikit-Learn
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix,
    brier_score_loss
)

# XGBoost
import xgboost as xgb
from xgboost import XGBClassifier

# PySpark
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento dos Dados da Camada Silver (Unity Catalog)

# COMMAND ----------

# Leitura direta das tabelas Delta gerenciadas no Unity Catalog
# Caso prefira ler do arquivo no Volume, pode alternar para spark.read.format("csv")...
try:
    df_sales_spark = spark.read.table("portfolio_lakehouse.silver.ft_vendas")
    dim_cli = spark.read.table("portfolio_lakehouse.silver.dim_cliente")
    dim_loc = spark.read.table("portfolio_lakehouse.silver.dim_localizacao")
    dim_prod = spark.read.table("portfolio_lakehouse.silver.dim_produto")
    
    # Enriquecimento com dimensões
    df_sales_spark = (
        df_sales_spark
        .join(dim_cli, "customer_name", "left")
        .join(dim_loc, ["country", "state"], "left")
        .join(dim_prod, "product_id", "left")
    )
    print("Carregado com sucesso a partir das tabelas Silver do Unity Catalog!")
except Exception as e:
    print(f"Fallback para leitura direta do Volume: {e}")
    df_sales_spark = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/portfolio_lakehouse/raw/raw_data/df_sales.csv")

# Conversão para Pandas para modelagem com Scikit-Learn / XGBoost
df_sales = df_sales_spark.toPandas()
df_sales['order_date'] = pd.to_datetime(df_sales['order_date'])
df_sales['sales'] = pd.to_numeric(df_sales['sales'], errors='coerce')
df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce')
df_sales['discount'] = pd.to_numeric(df_sales['discount'], errors='coerce')
df_sales['profit'] = pd.to_numeric(df_sales['profit'], errors='coerce')
df_sales['margem_lucro'] = pd.to_numeric(df_sales['margem_lucro'], errors='coerce')

print(f"Total de registros: {len(df_sales):,}")
print(f"Clientes únicos  : {df_sales['customer_name'].nunique():,}")
print(f"Período temporal : {df_sales['order_date'].min().strftime('%Y-%m-%d')} até {df_sales['order_date'].max().strftime('%Y-%m-%d')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construção do Datamart de Clientes (Feature Engineering)

# COMMAND ----------

def build_customer_features(df_transactions, reference_date=None):
    """
    Constrói as features comportamentais e transacionais (RFV) de cada cliente.
    """
    df = df_transactions.copy()
    if reference_date is not None:
        df = df[df['order_date'] <= pd.to_datetime(reference_date)]
        ref_dt = pd.to_datetime(reference_date)
    else:
        ref_dt = df['order_date'].max()

    features = []
    for customer, group in df.groupby('customer_name'):
        last_order = group['order_date'].max()
        first_order = group['order_date'].min()
        
        recency_days = (ref_dt - last_order).days
        frequency_orders = group['order_id'].nunique()
        monetary_value = group['sales'].sum()
        total_quantity = group['quantity'].sum()
        avg_ticket = monetary_value / frequency_orders if frequency_orders > 0 else 0.0
        avg_discount = group['discount'].mean()
        avg_profit_margin = group['margem_lucro'].mean()
        total_profit = group['profit'].sum()
        distinct_categories = group['category'].nunique() if 'category' in group else 1
        tenure_days = (last_order - first_order).days
        
        segment = group['segment'].mode()[0] if ('segment' in group and not group['segment'].empty) else 'Consumer'
        market = group['market'].mode()[0] if ('market' in group and not group['market'].empty) else 'Global'
        
        features.append({
            'customer_name': customer,
            'segment': segment,
            'market': market,
            'recencia_dias': recency_days,
            'frequencia_pedidos': frequency_orders,
            'valor_total_gasto': round(monetary_value, 2),
            'quantidade_itens': total_quantity,
            'ticket_medio': round(avg_ticket, 2),
            'lucro_total': round(total_profit, 2),
            'desconto_medio': round(avg_discount, 4),
            'margem_media': round(avg_profit_margin, 4),
            'qtd_categorias_distintas': distinct_categories,
            'tempo_relacionamento_dias': tenure_days,
            'data_primeira_compra': first_order.strftime('%Y-%m-%d'),
            'data_ultima_compra': last_order.strftime('%Y-%m-%d')
        })
        
    return pd.DataFrame(features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preparação do Treino com Janela Deslocada (Evitando Data Leakage)

# COMMAND ----------

# Ponto de corte histórico para treinar o modelo na capacidade de prever os próximos 180 dias
cutoff_date = "2014-06-30"
target_window_days = 180
cutoff_dt = pd.to_datetime(cutoff_date)
end_target_dt = cutoff_dt + pd.Timedelta(days=target_window_days)

# 1. Features baseadas apenas no passado (até o cutoff)
df_train_features = build_customer_features(df_sales, reference_date=cutoff_dt)

# 2. Alvo baseado no que aconteceu na janela futura
future_orders = df_sales[(df_sales['order_date'] > cutoff_dt) & (df_sales['order_date'] <= end_target_dt)]
buyers_in_window = set(future_orders['customer_name'].unique())

df_train_features['target_comprou'] = df_train_features['customer_name'].apply(
    lambda x: 1 if x in buyers_in_window else 0
)

print(f"Clientes analisados: {len(df_train_features)}")
print(f"Taxa de recompra no período alvo: {df_train_features['target_comprou'].mean()*100:.1f}%")
display(df_train_features.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline Scikit-Learn + XGBoost Classifier

# COMMAND ----------

numeric_features = [
    'recencia_dias', 'frequencia_pedidos', 'valor_total_gasto',
    'quantidade_itens', 'ticket_medio', 'lucro_total',
    'desconto_medio', 'margem_media', 'qtd_categorias_distintas',
    'tempo_relacionamento_dias'
]

categorical_features = ['segment', 'market']

X = df_train_features[numeric_features + categorical_features]
y = df_train_features['target_comprou']

# Split Treino / Teste estratificado
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numeric_features),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
    ]
)

# Ajuste de peso para classes desbalanceadas
scale_weight = (y_train == 0).sum() / (y_train == 1).sum()

xgb_model = XGBClassifier(
    n_estimators=150,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=scale_weight,
    eval_metric='logloss',
    random_state=42
)

pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', xgb_model)
])

# Cross-Validation 5-Fold
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(pipeline, X_train, y_train, cv=cv, scoring='roc_auc')
print(f"ROC-AUC Médio no Cross-Validation: {cv_scores.mean():.4f} (± {cv_scores.std():.4f})")

# Fit final
pipeline.fit(X_train, y_train)

# Avaliação no conjunto de teste
y_pred = pipeline.predict(X_test)
y_proba = pipeline.predict_proba(X_test)[:, 1]

print("\n--- PERFORMANCE NO TESTE (HOLDOUT) ---")
print(f"ROC-AUC: {roc_auc_score(y_test, y_proba):.4f}")
print(f"Brier Score: {brier_score_loss(y_test, y_proba):.4f}")
print("\nRelatório de Classificação:")
print(classification_report(y_test, y_pred, target_names=['Não Comprou', 'Comprou']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Importância das Variáveis (Feature Importance)

# COMMAND ----------

# Extrai os nomes das colunas processadas
ohe_cols = pipeline.named_steps['preprocessor'].named_transformers_['cat'].get_feature_names_out(categorical_features).tolist()
all_feature_names = numeric_features + ohe_cols

importances = pipeline.named_steps['classifier'].feature_importances_
df_importance = pd.DataFrame({
    'Feature': all_feature_names,
    'Importance': importances
}).sort_values(by='Importance', ascending=False)

print("Principais variáveis que determinam a propensão de compra:")
display(df_importance.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Scoring de Propensão para 100% da Base Atual de Clientes

# COMMAND ----------

# Gera as features considerando todo o histórico até a data mais recente
df_current_features = build_customer_features(df_sales, reference_date=None)

X_current = df_current_features[numeric_features + categorical_features]
scores = pipeline.predict_proba(X_current)[:, 1]

df_current_features['propensity_score'] = np.round(scores, 4)
df_current_features['percentil_score'] = pd.qcut(
    df_current_features['propensity_score'], q=10, labels=False, duplicates='drop'
) + 1

# Segmentação de Ação de Negócio para o CRM
def segment_action(score):
    if score >= 0.70:
        return '1. Alta Propensão (Conversão Imediata / Oferta Direta)'
    elif score >= 0.40:
        return '2. Média Propensão (Nutrição / Engajamento)'
    else:
        return '3. Baixa Propensão (Reativação / Churn Risk)'

df_current_features['estrategia_crm'] = df_current_features['propensity_score'].apply(segment_action)

# Ordenação estrita do maior score para o menor
df_ranked = df_current_features.sort_values(by='propensity_score', ascending=False).reset_index(drop=True)
df_ranked['ranking'] = df_ranked.index + 1

display(df_ranked[[
    'ranking', 'customer_name', 'propensity_score', 'estrategia_crm',
    'recencia_dias', 'frequencia_pedidos', 'valor_total_gasto', 'ticket_medio',
    'data_ultima_compra', 'segment'
]].head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Persistência na Camada Gold do Unity Catalog

# COMMAND ----------

# Conversão de volta para PySpark DataFrame
spark_scored_df = spark.createDataFrame(df_ranked)

# Gravação gerenciada no schema Gold
target_gold_table = "portfolio_lakehouse.gold.dm_propensao_compra_clientes"

(
    spark_scored_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_gold_table)
)

print(f"✅ Tabela salva com sucesso no Unity Catalog: {target_gold_table}")
