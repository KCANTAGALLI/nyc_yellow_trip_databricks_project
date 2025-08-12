# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Yellow Trip Records - Camada Silver (Tratamento e Limpeza)
# MAGIC 
# MAGIC **Objetivo:** Processar e limpar dados da camada Bronze, aplicando regras de negócio e validações
# MAGIC 
# MAGIC **Características da Camada Silver:**
# MAGIC - Dados limpos e validados
# MAGIC - Aplicação de regras de negócio
# MAGIC - Remoção de outliers e dados inconsistentes
# MAGIC - Enriquecimento com informações derivadas
# MAGIC - Particionamento otimizado para consultas analíticas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime, timedelta
import numpy as np

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações e Parâmetros

# COMMAND ----------

# Configurações do projeto
BRONZE_TABLE = "bronze.yellow_tripdata"
SILVER_TABLE = "silver.yellow_tripdata_clean"
SILVER_DELTA_PATH = "/delta/silver/yellow_tripdata_clean/"

# Parâmetros de limpeza e validação
CLEANING_RULES = {
    # Limites geográficos de NYC (aproximados)
    "lat_min": 40.477399,
    "lat_max": 40.917577,
    "lon_min": -74.259090,
    "lon_max": -73.700272,
    
    # Limites de valores monetários
    "fare_min": 0.0,
    "fare_max": 1000.0,
    "tip_max": 500.0,
    "total_max": 1500.0,
    
    # Limites de distância e tempo
    "distance_min": 0.0,
    "distance_max": 500.0,  # milhas
    "duration_min": 60,     # segundos (1 minuto)
    "duration_max": 86400,  # segundos (24 horas)
    
    # Limites de passageiros
    "passenger_min": 0,
    "passenger_max": 6,
    
    # Velocidade máxima (mph)
    "max_speed": 200
}

# Mapeamentos de códigos
VENDOR_MAPPING = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc"
}

RATECODE_MAPPING = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}

PAYMENT_TYPE_MAPPING = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

print("Configurações carregadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções de Limpeza e Validação

# COMMAND ----------

def add_derived_columns(df):
    """
    Adiciona colunas derivadas úteis para análise
    """
    df_enhanced = df.withColumn(
        # Duração da viagem em minutos
        "trip_duration_minutes", 
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    ).withColumn(
        # Velocidade média (mph)
        "avg_speed_mph",
        when(col("trip_duration_minutes") > 0, 
             col("trip_distance") / (col("trip_duration_minutes") / 60))
        .otherwise(0)
    ).withColumn(
        # Hora do dia do pickup
        "pickup_hour", hour("tpep_pickup_datetime")
    ).withColumn(
        # Dia da semana (1=Segunda, 7=Domingo)
        "pickup_dayofweek", dayofweek("tpep_pickup_datetime")
    ).withColumn(
        # Nome do dia da semana
        "pickup_dayname", 
        when(dayofweek("tpep_pickup_datetime") == 1, "Sunday")
        .when(dayofweek("tpep_pickup_datetime") == 2, "Monday")
        .when(dayofweek("tpep_pickup_datetime") == 3, "Tuesday")
        .when(dayofweek("tpep_pickup_datetime") == 4, "Wednesday")
        .when(dayofweek("tpep_pickup_datetime") == 5, "Thursday")
        .when(dayofweek("tpep_pickup_datetime") == 6, "Friday")
        .when(dayofweek("tpep_pickup_datetime") == 7, "Saturday")
    ).withColumn(
        # Mês e ano
        "pickup_month", month("tpep_pickup_datetime")
    ).withColumn(
        "pickup_year", year("tpep_pickup_datetime")
    ).withColumn(
        # Período do dia
        "time_period",
        when(col("pickup_hour").between(6, 11), "Morning")
        .when(col("pickup_hour").between(12, 17), "Afternoon")
        .when(col("pickup_hour").between(18, 21), "Evening")
        .otherwise("Night")
    ).withColumn(
        # Fim de semana
        "is_weekend",
        when(col("pickup_dayofweek").isin([1, 7]), True).otherwise(False)
    ).withColumn(
        # Taxa de gorjeta (%)
        "tip_percentage",
        when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
        .otherwise(0)
    )
    
    return df_enhanced

def apply_data_quality_filters(df):
    """
    Aplica filtros de qualidade de dados
    """
    rules = CLEANING_RULES
    
    # Filtros de validação
    df_clean = df.filter(
        # Timestamps válidos
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
        
        # Valores monetários válidos
        (col("fare_amount") >= rules["fare_min"]) &
        (col("fare_amount") <= rules["fare_max"]) &
        (col("total_amount") >= 0) &
        (col("total_amount") <= rules["total_max"]) &
        (col("tip_amount") >= 0) &
        (col("tip_amount") <= rules["tip_max"]) &
        
        # Distância válida
        (col("trip_distance") >= rules["distance_min"]) &
        (col("trip_distance") <= rules["distance_max"]) &
        
        # Duração válida
        (col("trip_duration_minutes") >= rules["duration_min"]/60) &
        (col("trip_duration_minutes") <= rules["duration_max"]/60) &
        
        # Passageiros válidos
        (col("passenger_count") >= rules["passenger_min"]) &
        (col("passenger_count") <= rules["passenger_max"]) &
        
        # Velocidade razoável
        (col("avg_speed_mph") <= rules["max_speed"]) &
        
        # IDs de localização válidos (não nulos)
        (col("PULocationID").isNotNull()) &
        (col("DOLocationID").isNotNull()) &
        (col("PULocationID") > 0) &
        (col("DOLocationID") > 0) &
        
        # Vendor ID válido
        (col("VendorID").isin([1, 2]))
    )
    
    return df_clean

def add_business_mappings(df):
    """
    Adiciona mapeamentos de códigos para descrições legíveis
    """
    # Criar DataFrames de mapeamento
    vendor_df = spark.createDataFrame(
        [(k, v) for k, v in VENDOR_MAPPING.items()],
        ["VendorID", "vendor_name"]
    )
    
    ratecode_df = spark.createDataFrame(
        [(k, v) for k, v in RATECODE_MAPPING.items()],
        ["RatecodeID", "rate_code_desc"]
    )
    
    payment_df = spark.createDataFrame(
        [(k, v) for k, v in PAYMENT_TYPE_MAPPING.items()],
        ["payment_type", "payment_type_desc"]
    )
    
    # Aplicar joins
    df_mapped = df.join(vendor_df, "VendorID", "left") \
                 .join(ratecode_df, df.RatecodeID == ratecode_df.RatecodeID, "left") \
                 .join(payment_df, "payment_type", "left") \
                 .drop(ratecode_df.RatecodeID)
    
    return df_mapped

def detect_and_flag_outliers(df):
    """
    Detecta e marca outliers usando método IQR
    """
    # Definir colunas para análise de outliers
    outlier_columns = ["trip_distance", "fare_amount", "tip_amount", "total_amount", "trip_duration_minutes"]
    
    df_with_outliers = df
    
    for col_name in outlier_columns:
        # Calcular quartis
        quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
        if len(quantiles) == 2:
            q1, q3 = quantiles
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Adicionar flag de outlier
            df_with_outliers = df_with_outliers.withColumn(
                f"{col_name}_outlier",
                when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), True)
                .otherwise(False)
            )
    
    # Flag geral de outlier
    outlier_flags = [f"{col_name}_outlier" for col_name in outlier_columns]
    df_with_outliers = df_with_outliers.withColumn(
        "has_outlier",
        expr(" OR ".join(outlier_flags))
    )
    
    return df_with_outliers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carregamento e Processamento dos Dados Bronze

# COMMAND ----------

print("Carregando dados da camada Bronze...")

# Carregar dados da camada Bronze
bronze_df = spark.table(BRONZE_TABLE)

print(f"Registros na camada Bronze: {bronze_df.count():,}")

# Verificar período dos dados
date_stats = bronze_df.agg(
    min("tpep_pickup_datetime").alias("min_date"),
    max("tpep_pickup_datetime").alias("max_date"),
    countDistinct("ingestion_month").alias("distinct_months")
).collect()[0]

print(f"Período: {date_stats['min_date']} a {date_stats['max_date']}")
print(f"Meses distintos: {date_stats['distinct_months']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aplicação das Transformações

# COMMAND ----------

print("Aplicando transformações da camada Silver...")

# Passo 1: Adicionar colunas derivadas
print("   1. Adicionando colunas derivadas...")
df_step1 = add_derived_columns(bronze_df)

# Passo 2: Aplicar filtros de qualidade
print("   2. Aplicando filtros de qualidade...")
records_before = df_step1.count()
df_step2 = apply_data_quality_filters(df_step1)
records_after = df_step2.count()
records_removed = records_before - records_after

print(f"      Registros removidos: {records_removed:,} ({(records_removed/records_before)*100:.2f}%)")

# Passo 3: Adicionar mapeamentos de negócio
print("   3. Adicionando mapeamentos de negócio...")
df_step3 = add_business_mappings(df_step2)

# Passo 4: Detectar outliers
print("   4. Detectando outliers...")
df_step4 = detect_and_flag_outliers(df_step3)

# Passo 5: Adicionar metadados de processamento
print("   5. Adicionando metadados de processamento...")
df_final = df_step4.withColumn("silver_processing_timestamp", current_timestamp()) \
                   .withColumn("silver_processing_version", lit("1.0"))

print(f"Processamento concluído: {df_final.count():,} registros limpos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análise de Qualidade Pós-Processamento

# COMMAND ----------

print("ANÁLISE DE QUALIDADE - CAMADA SILVER")
print("=" * 45)

# Estatísticas gerais
total_records = df_final.count()
outlier_records = df_final.filter(col("has_outlier") == True).count()

print(f"Total de registros: {total_records:,}")
print(f"Registros com outliers: {outlier_records:,} ({(outlier_records/total_records)*100:.2f}%)")

# Distribuição por mês
monthly_distribution = df_final.groupBy("pickup_month", "pickup_year") \
                              .count() \
                              .orderBy("pickup_year", "pickup_month")

print("\nDistribuição por mês:")
monthly_distribution.show()

# Estatísticas de valores monetários
monetary_stats = df_final.select(
    avg("fare_amount").alias("avg_fare"),
    stddev("fare_amount").alias("std_fare"),
    avg("tip_amount").alias("avg_tip"),
    avg("tip_percentage").alias("avg_tip_pct"),
    avg("total_amount").alias("avg_total"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration")
).collect()[0]

print("\nEstatísticas monetárias:")
print(f"   Tarifa média: ${monetary_stats['avg_fare']:.2f} (±${monetary_stats['std_fare']:.2f})")
print(f"   Gorjeta média: ${monetary_stats['avg_tip']:.2f}")
print(f"   % gorjeta média: {monetary_stats['avg_tip_pct']:.1f}%")
print(f"   Total médio: ${monetary_stats['avg_total']:.2f}")

print(f"\n🚗 Estatísticas de viagem:")
print(f"   Distância média: {monetary_stats['avg_distance']:.2f} milhas")
print(f"   Duração média: {monetary_stats['avg_duration']:.1f} minutos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análises Específicas de Negócio

# COMMAND ----------

print("ANÁLISES DE NEGÓCIO")
print("=" * 25)

# Distribuição por vendor
vendor_analysis = df_final.groupBy("vendor_name") \
                          .agg(count("*").alias("trips"),
                               avg("fare_amount").alias("avg_fare"),
                               avg("tip_percentage").alias("avg_tip_pct")) \
                          .orderBy(desc("trips"))

print("Análise por Vendor:")
vendor_analysis.show()

# Padrões temporais
time_patterns = df_final.groupBy("time_period") \
                       .agg(count("*").alias("trips"),
                            avg("fare_amount").alias("avg_fare"),
                            avg("trip_distance").alias("avg_distance")) \
                       .orderBy(desc("trips"))

print("Padrões por período do dia:")
time_patterns.show()

# Análise fim de semana vs dias úteis
weekend_analysis = df_final.groupBy("is_weekend") \
                          .agg(count("*").alias("trips"),
                               avg("fare_amount").alias("avg_fare"),
                               avg("tip_percentage").alias("avg_tip_pct"),
                               avg("trip_distance").alias("avg_distance")) \
                          .orderBy("is_weekend")

print("Fim de semana vs Dias úteis:")
weekend_analysis.show()

# Top 10 rotas (PU -> DO)
top_routes = df_final.groupBy("PULocationID", "DOLocationID") \
                    .count() \
                    .orderBy(desc("count")) \
                    .limit(10)

print("Top 10 rotas mais frequentes:")
top_routes.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Salvamento na Camada Silver

# COMMAND ----------

# Criar database se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

print("Salvando dados na camada Silver...")

# Salvar tabela particionada por ano e mês para otimizar consultas analíticas
df_final.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("pickup_year", "pickup_month") \
    .format("delta") \
    .saveAsTable(SILVER_TABLE)

print(f"Dados salvos na tabela: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Otimização da Tabela Delta

# COMMAND ----------

print("Otimizando tabela Silver...")

# OPTIMIZE com ZORDER nas colunas mais consultadas
spark.sql(f"""
OPTIMIZE {SILVER_TABLE}
ZORDER BY (tpep_pickup_datetime, PULocationID, DOLocationID, VendorID)
""")

# Estatísticas da tabela para otimização de consultas
spark.sql(f"ANALYZE TABLE {SILVER_TABLE} COMPUTE STATISTICS FOR ALL COLUMNS")

print("Otimização concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verificação Final e Metadados

# COMMAND ----------

# Verificar tabela criada
silver_df = spark.table(SILVER_TABLE)

print("INFORMAÇÕES FINAIS - CAMADA SILVER")
print("=" * 40)

print(f"Total de registros: {silver_df.count():,}")
print(f"Número de colunas: {len(silver_df.columns)}")

# Mostrar algumas colunas adicionadas
new_columns = [
    "trip_duration_minutes", "avg_speed_mph", "pickup_hour", 
    "time_period", "is_weekend", "tip_percentage", 
    "vendor_name", "payment_type_desc", "has_outlier"
]

print(f"\nColunas adicionadas na camada Silver:")
for col in new_columns:
    if col in silver_df.columns:
        print(f"   {col}")

# Schema resumido
print(f"\nSchema da tabela Silver (primeiras 20 colunas):")
silver_df.select(*silver_df.columns[:20]).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Relatório de Transformações Aplicadas

# COMMAND ----------

print("RELATÓRIO DE TRANSFORMAÇÕES - CAMADA SILVER")
print("=" * 50)

print("Transformações aplicadas:")
print("   1. Adição de colunas derivadas:")
print("      - Duração da viagem (minutos)")
print("      - Velocidade média (mph)")
print("      - Informações temporais (hora, dia da semana, período)")
print("      - Percentual de gorjeta")
print("      - Flags de fim de semana")
print("")
print("   2. Filtros de qualidade de dados:")
print("      - Remoção de timestamps inválidos")
print("      - Filtros de valores monetários extremos")
print("      - Validação de distâncias e durações")
print("      - Limitação de velocidades irreais")
print("      - Validação de IDs de localização")
print("")
print("   3. Enriquecimento de dados:")
print("      - Mapeamento de códigos de vendor")
print("      - Descrições de tipos de pagamento")
print("      - Descrições de códigos de tarifa")
print("")
print("   4. Detecção de outliers:")
print("      - Análise IQR para valores monetários")
print("      - Flags de outliers por coluna")
print("      - Flag geral de outlier")
print("")
print("   5. Particionamento otimizado:")
print("      - Partições por ano e mês de pickup")
print("      - ZORDER por colunas frequentemente consultadas")

# Estatísticas de limpeza
bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = silver_df.count()
data_quality_rate = (silver_count / bronze_count) * 100

print(f"\nEstatísticas de limpeza:")
print(f"   Registros Bronze: {bronze_count:,}")
print(f"   Registros Silver: {silver_count:,}")
print(f"   Taxa de qualidade: {data_quality_rate:.2f}%")
print(f"   Registros removidos: {bronze_count - silver_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Camada Silver
# MAGIC 
# MAGIC ### Processamento Realizado:
# MAGIC - **Limpeza de dados:** Aplicação de filtros de qualidade rigorosos
# MAGIC - **Enriquecimento:** Adição de 15+ colunas derivadas e mapeamentos
# MAGIC - **Validação:** Remoção de outliers e dados inconsistentes
# MAGIC - **Otimização:** Particionamento por ano/mês e ZORDER para performance
# MAGIC 
# MAGIC ### Colunas Adicionadas:
# MAGIC - Métricas temporais: duração, velocidade, períodos do dia
# MAGIC - Informações de negócio: descrições de códigos, percentual de gorjeta
# MAGIC - Flags de qualidade: outliers, fim de semana, validações
# MAGIC - Metadados: timestamps de processamento, versão
# MAGIC 
# MAGIC ### Controles de Qualidade:
# MAGIC - Taxa de qualidade > 90% dos dados originais
# MAGIC - Detecção automática de outliers
# MAGIC - Validação de regras de negócio
# MAGIC - Relatórios detalhados de transformações
# MAGIC 
# MAGIC ### Próximos Passos:
# MAGIC 1. Executar notebook `03_analise_gold.py` para métricas e KPIs
# MAGIC 2. Criar visualizações e dashboards analíticos
# MAGIC 3. Implementar monitoramento de qualidade contínuo
# MAGIC 
# MAGIC ### Localização dos Dados:
# MAGIC - **Tabela:** `silver.yellow_tripdata_clean`
# MAGIC - **Particionamento:** `pickup_year`, `pickup_month`
# MAGIC - **Otimizações:** ZORDER por colunas de consulta frequente

# COMMAND ----------

print("SILVER PROCESSING COMPLETED SUCCESSFULLY")
print("Próximo passo: Execute o notebook 03_analise_gold.py")
