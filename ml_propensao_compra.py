"""
=============================================================================
MODELO DE PROPENSÃO DE COMPRA (PURCHASE PROPENSITY SCORE)
TECNOLOGIAS: XGBoost, Scikit-Learn, Pandas, NumPy
AUTOR: Rafael Corrêa | Engenharia de Dados & Machine Learning
=============================================================================

ESTRUTURA DO PROJETO:
1. Construção do Datamart de Clientes (Feature Engineering: RFV + Métricas Avançadas)
2. Definição da Janela Temporal (Observation Window vs. Prediction Window) para evitar Data Leakage
3. Treinamento e Validação com Pipeline do Scikit-Learn + XGBClassifier
4. Avaliação de Performance (ROC-AUC, Precision, Recall, Matriz de Confusão, Feature Importance)
5. Scoring de Propensão (0 a 1) para 100% da base de clientes
6. Exportação do Ranking Ordenado com Faixas de Ação de Negócio (Alta, Média e Baixa Propensão)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

# Scikit-Learn para split, pré-processamento e métricas
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    roc_auc_score,
    classification_report,
    confusion_matrix,
    brier_score_loss,
    roc_curve
)

# XGBoost
import xgboost as xgb
from xgboost import XGBClassifier


# =============================================================================
# 1. FUNÇÃO: CONSTRUÇÃO DO DATAMART DE CLIENTES (FEATURE STORE ANALÍTICA)
# =============================================================================
def build_customer_features(df_transactions, reference_date=None):
    """
    Constrói o Datamart analítico de clientes a partir do histórico transacional.
    
    Parâmetros:
    - df_transactions: DataFrame com as transações históricas de vendas.
    - reference_date: Data limite (cutoff) para cálculo das métricas.
                      Se None, usa a data máxima do DataFrame.
    
    Retorna:
    - DataFrame agregado por cliente com Recência, Frequência, Valor e Comportamento.
    """
    df = df_transactions.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    if reference_date is not None:
        df = df[df['order_date'] <= pd.to_datetime(reference_date)]
        ref_dt = pd.to_datetime(reference_date)
    else:
        ref_dt = df['order_date'].max()

    # Agregação por cliente
    features = []
    
    for customer, group in df.groupby('customer_name'):
        # Última e primeira data de compra
        last_order = group['order_date'].max()
        first_order = group['order_date'].min()
        
        # 1. Recência: dias decorridos desde a última compra
        recency_days = (ref_dt - last_order).days
        
        # 2. Frequência: quantidade de pedidos únicos
        frequency_orders = group['order_id'].nunique()
        
        # 3. Valor: soma total de faturamento gerado
        monetary_value = group['sales'].sum()
        
        # 4. Volume físico: quantidade total de unidades
        total_quantity = group['quantity'].sum()
        
        # 5. Ticket Médio
        avg_ticket = monetary_value / frequency_orders if frequency_orders > 0 else 0.0
        
        # 6. Desconto médio e Margem média
        avg_discount = group['discount'].mean()
        avg_profit_margin = group['margem_lucro'].mean()
        total_profit = group['profit'].sum()
        
        # 7. Diversidade de compra (número de categorias e produtos distintos)
        distinct_categories = group['category'].nunique()
        distinct_subcategories = group['sub_category'].nunique()
        distinct_products = group['product_id'].nunique()
        
        # 8. Tempo de Vida / Relacionamento (Tenure em dias)
        tenure_days = (last_order - first_order).days
        
        # 9. Segmento e Mercado predominante
        customer_segment = group['segment'].mode()[0] if not group['segment'].empty else 'Unknown'
        primary_market = group['market'].mode()[0] if not group['market'].empty else 'Unknown'
        
        features.append({
            'customer_name': customer,
            'segment': customer_segment,
            'market': primary_market,
            'recencia_dias': recency_days,
            'frequencia_pedidos': frequency_orders,
            'valor_total_gasto': round(monetary_value, 2),
            'quantidade_itens': total_quantity,
            'ticket_medio': round(avg_ticket, 2),
            'lucro_total': round(total_profit, 2),
            'desconto_medio': round(avg_discount, 4),
            'margem_media': round(avg_profit_margin, 4),
            'qtd_categorias_distintas': distinct_categories,
            'qtd_subcategorias_distintas': distinct_subcategories,
            'qtd_produtos_distintos': distinct_products,
            'tempo_relacionamento_dias': tenure_days,
            'data_primeira_compra': first_order.strftime('%Y-%m-%d'),
            'data_ultima_compra': last_order.strftime('%Y-%m-%d')
        })
        
    return pd.DataFrame(features)


# =============================================================================
# 2. PREPARAÇÃO DO DATASET DE TREINAMENTO (MULTI-WINDOW STRATEGY)
# =============================================================================
def prepare_training_dataset(df_transactions, target_window_days=90):
    """
    Estratégia Multi-Janela para gerar treino balanceado e realista:
    Itera sobre múltiplos pontos de corte (rolling windows anuais) e agrega
    todos os snapshots em uma base de treino única e robusta.

    Para cada janela:
    - Features: comportamento acumulado até o cutoff
    - Target: 1 se o cliente comprou nos próximos `target_window_days`, 0 caso contrário

    Isso resolve o problema de classes degeneradas em janelas muito próximas
    ao final do período histórico e enriquece o modelo com sazonalidade.
    """
    df = df_transactions.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Pontos de corte distribuídos ao longo do histórico (2012–2013)
    # Janelas intermediárias garantem histórico suficiente E dados futuros para o target
    cutoff_dates = [
        "2012-06-30",   # 18 meses de histórico → target até set/2012
        "2012-12-31",   # 24 meses de histórico → target até mar/2013
        "2013-06-30",   # 30 meses de histórico → target até set/2013
        "2013-12-31",   # 36 meses de histórico → target até mar/2014
    ]

    all_windows = []
    print(f"\n[INFO] Estratégia Multi-Janela com {len(cutoff_dates)} pontos de corte (target_window={target_window_days} dias)")

    for cutoff_date in cutoff_dates:
        cutoff_dt = pd.to_datetime(cutoff_date)
        end_target_dt = cutoff_dt + pd.Timedelta(days=target_window_days)

        # Só inclui clientes com atividade ANTES do cutoff
        clientes_ativos = df[df['order_date'] <= cutoff_dt]['customer_name'].unique()
        if len(clientes_ativos) == 0:
            continue

        df_features = build_customer_features(df, reference_date=cutoff_dt)

        # Target: comprou nos próximos N dias após o cutoff?
        future_orders = df[(df['order_date'] > cutoff_dt) & (df['order_date'] <= end_target_dt)]
        buyers_in_window = set(future_orders['customer_name'].unique())

        df_features['target_comprou'] = df_features['customer_name'].apply(
            lambda x: 1 if x in buyers_in_window else 0
        )
        df_features['cutoff'] = cutoff_date

        conv = df_features['target_comprou'].mean() * 100
        print(f"  Cutoff {cutoff_date}: {len(df_features)} clientes | {df_features['target_comprou'].sum()} compraram ({conv:.1f}%)")
        all_windows.append(df_features)

    df_train = pd.concat(all_windows, ignore_index=True)
    df_train = df_train.drop(columns=['cutoff'])

    print(f"\n[INFO] Dataset final de treino: {len(df_train)} amostras")
    print(f"[INFO] Taxa global de compra: {df_train['target_comprou'].mean()*100:.1f}%")
    print(f"[INFO] Positivos: {df_train['target_comprou'].sum()} | Negativos: {(df_train['target_comprou'] == 0).sum()}")

    return df_train



# =============================================================================
# 3. PIPELINE DE MACHINE LEARNING COM SCIKIT-LEARN & XGBOOST
# =============================================================================
def train_propensity_model(df_train):
    """
    Treina o modelo XGBoost com pré-processamento via Scikit-Learn Pipeline.
    """
    # Definição das colunas de features
    numeric_features = [
        'recencia_dias',
        'frequencia_pedidos',
        'valor_total_gasto',
        'quantidade_itens',
        'ticket_medio',
        'lucro_total',
        'desconto_medio',
        'margem_media',
        'qtd_categorias_distintas',
        'qtd_subcategorias_distintas',
        'qtd_produtos_distintos',
        'tempo_relacionamento_dias'
    ]
    
    categorical_features = ['segment', 'market']
    
    X = df_train[numeric_features + categorical_features]
    y = df_train['target_comprou']
    
    # Divisão em Treino (80%) e Teste (20%) estratificada
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    
    # Pré-processamento
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ]
    )
    
    # Cálculo do scale_pos_weight para balanceamento de classes
    num_neg = (y_train == 0).sum()
    num_pos = (y_train == 1).sum()
    scale_weight = num_neg / num_pos if num_pos > 0 else 1.0
    
    # Definição do classificador XGBoost
    xgb_classifier = XGBClassifier(
        n_estimators=150,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_weight,
        eval_metric='logloss',
        random_state=42
    )
    
    # Pipeline Scikit-Learn completo
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', xgb_classifier)
    ])
    
    print("\n[INFO] Treinando modelo XGBoost com Validação Cruzada (5-Fold)...")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_auc = cross_val_score(model_pipeline, X_train, y_train, cv=cv, scoring='roc_auc')
    print(f"[RESULTADO] ROC-AUC Médio no Cross-Validation: {cv_auc.mean():.4f} (± {cv_auc.std():.4f})")
    
    # Ajuste final nos dados de treino
    model_pipeline.fit(X_train, y_train)
    
    # Avaliação no conjunto de Teste (Dados Inéditos)
    y_pred = model_pipeline.predict(X_test)
    y_proba = model_pipeline.predict_proba(X_test)[:, 1]
    test_auc = roc_auc_score(y_test, y_proba)
    brier = brier_score_loss(y_test, y_proba)
    
    print(f"\n" + "="*60)
    print(f" AVALIAÇÃO DE PERFORMANCE NO CONJUNTO DE TESTE (HOLDOUT)")
    print(f"="*60)
    print(f"• ROC-AUC Score : {test_auc:.4f}")
    print(f"• Brier Score   : {brier:.4f} (Calibração de Probabilidade - quanto menor, melhor)")
    print("\n[Relatório de Classificação]")
    print(classification_report(y_test, y_pred, target_names=['Não Comprou (0)', 'Comprou (1)']))
    
    print("[Matriz de Confusão]")
    cm = confusion_matrix(y_test, y_pred)
    print(f"Verdadeiros Negativos : {cm[0,0]} | Falsos Positivos : {cm[0,1]}")
    print(f"Falsos Negativos      : {cm[1,0]} | Verdadeiros Positivos: {cm[1,1]}")
    print(f"="*60)
    
    return model_pipeline, numeric_features, categorical_features


# =============================================================================
# 4. SCORING DE PROPENSÃO EM TODA A BASE DE CLIENTES
# =============================================================================
def generate_propensity_scores(model_pipeline, df_full_history, numeric_features, categorical_features):
    """
    Gera o score de propensão atualizado (0 a 1) para todos os clientes da base
    considerando o histórico completo mais recente.
    """
    print("\n[INFO] Construindo Datamart Atual com histórico completo...")
    df_current_features = build_customer_features(df_full_history, reference_date=None)
    
    X_current = df_current_features[numeric_features + categorical_features]
    
    # Predição da probabilidade de compra (Score entre 0 e 1)
    propensity_scores = model_pipeline.predict_proba(X_current)[:, 1]
    
    df_current_features['propensity_score'] = np.round(propensity_scores, 4)
    df_current_features['percentil_score'] = pd.qcut(
        df_current_features['propensity_score'], q=10, labels=False, duplicates='drop'
    ) + 1  # Decil de 1 a 10 (10 = maior propensão)
    
    # Classificação em Faixas de Ação de Negócio (Clusters de CRM)
    def categorize_propensity(score):
        if score >= 0.70:
            return 'Alta Propensão (Foco em Conversão)'
        elif score >= 0.40:
            return 'Média Propensão (Foco em Nutrição)'
        else:
            return 'Baixa Propensão (Foco em Reativação)'
            
    df_current_features['faixa_propensao'] = df_current_features['propensity_score'].apply(categorize_propensity)
    
    # Ordena rigorosamente do maior score para o menor
    df_ranked = df_current_features.sort_values(by='propensity_score', ascending=False).reset_index(drop=True)
    df_ranked['ranking'] = df_ranked.index + 1
    
    return df_ranked


# =============================================================================
# 5. EXECUÇÃO PRINCIPAL (MAIN PIPELINE)
# =============================================================================
def run_propensity_pipeline(csv_path="df_sales.csv", output_csv="dm_propensao_compra_clientes.csv"):
    print("="*70)
    print(" PIPELINE DE PROPENSÃO DE COMPRA: DATA LAKEHOUSE + XGBOOST ")
    print("="*70)
    
    # 1. Carregamento dos dados brutos
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
        
    print(f"[1/5] Carregando histórico de vendas de: {csv_path}")
    df_sales = pd.read_csv(csv_path)
    
    # Tratamento inicial de tipos caso venha bruto
    df_sales['sales'] = pd.to_numeric(df_sales['sales'], errors='coerce')
    df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce')
    df_sales['discount'] = pd.to_numeric(df_sales['discount'], errors='coerce')
    df_sales['profit'] = pd.to_numeric(df_sales['profit'], errors='coerce')
    df_sales['margem_lucro'] = pd.to_numeric(df_sales['margem_lucro'], errors='coerce')
    df_sales = df_sales.dropna(subset=['order_id', 'customer_name', 'order_date', 'sales'])
    
    # 2. Definição do Ponto de Corte Temporal
    # Estratégia Multi-Janela: 4 snapshots históricos (2012-2013) para treino robusto

    # 3. Construção do Dataset de Treino sem Data Leakage
    print(f"\n[2/5] Construindo Datamart Histórico com Estratégia Multi-Janela...")
    df_train = prepare_training_dataset(df_sales, target_window_days=90)
    
    # 4. Treinamento do Modelo com XGBoost & Scikit-Learn
    print(f"\n[3/5] Treinando Modelo de Propensão (XGBoost Classifier)...")
    model, num_cols, cat_cols = train_propensity_model(df_train)
    
    # 5. Geração de Scores para 100% da Base de Clientes (Tempo Presente)
    print(f"\n[4/5] Gerando Scores de Propensão para todos os clientes...")
    df_scored = generate_propensity_scores(model, df_sales, num_cols, cat_cols)
    
    # 6. Exibição e Exportação dos Resultados
    print(f"\n[5/5] Exportando Datamart Ranqueado para: {output_csv}")
    cols_to_export = [
        'ranking',
        'customer_name',
        'propensity_score',
        'faixa_propensao',
        'percentil_score',
        'recencia_dias',
        'frequencia_pedidos',
        'valor_total_gasto',
        'ticket_medio',
        'data_ultima_compra',
        'segment',
        'market'
    ]
    df_output = df_scored[cols_to_export]
    df_output.to_csv(output_csv, index=False, encoding='utf-8')
    
    # Prévia dos Top 10 Clientes com Maior Propensão
    print("\n" + "="*70)
    print(" TOP 10 CLIENTES COM MAIOR PROPENSÃO DE COMPRA (SCORE MÁXIMO)")
    print("="*70)
    print(df_output.head(10).to_string(index=False))
    
    # Distribuição por Faixa de Propensão
    print("\n" + "="*70)
    print(" DISTRIBUIÇÃO DA BASE POR FAIXA DE PROPENSÃO (ESTRATÉGIA DE CRM)")
    print("="*70)
    resumo_faixas = df_output.groupby('faixa_propensao').agg(
        total_clientes=('customer_name', 'count'),
        ticket_medio_medio=('ticket_medio', 'mean'),
        valor_acumulado=('valor_total_gasto', 'sum')
    ).reset_index()
    resumo_faixas['%_base'] = np.round((resumo_faixas['total_clientes'] / len(df_output)) * 100, 1)
    print(resumo_faixas.to_string(index=False))
    print("="*70)
    print(f"\n[OK] Concluido com sucesso! Arquivo gerado: {output_csv}")
    
    return df_output


if __name__ == "__main__":
    run_propensity_pipeline()
