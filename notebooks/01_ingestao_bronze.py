# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Yellow Trip Records - Camada Bronze (Ingestão)
# MAGIC 
# MAGIC **Objetivo:** Ingerir dados brutos dos Yellow Trip Records de NYC (jan-abr/2023) na camada Bronze
# MAGIC 
# MAGIC **Fonte:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# MAGIC 
# MAGIC **Características da Camada Bronze:**
# MAGIC - Dados brutos, sem transformações
# MAGIC - Preservação da estrutura original
# MAGIC - Armazenamento em formato Delta Lake
# MAGIC - Particionamento por data de pickup
# MAGIC - Controle de qualidade básico

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações do Ambiente

# COMMAND ----------

# Configurações do projeto
PROJECT_NAME = "nyc_yellow_trip"
BRONZE_TABLE = "bronze.yellow_tripdata"
DATA_PATH = "/FileStore/shared_uploads/data/yellow_tripdata_2023/"
DELTA_PATH_BRONZE = "/delta/bronze/yellow_tripdata/"

# URLs dos dados (jan-abr 2023)
DATA_URLS = {
    "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
    "2023-03": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
    "2023-04": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet"
}

# Schema esperado dos dados
EXPECTED_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

print("Configurações carregadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções Auxiliares

# COMMAND ----------

def download_data_if_needed(url: str, local_path: str) -> bool:
    """
    Baixa dados se não existirem localmente
    """
    try:
        # Verifica se o arquivo já existe
        if os.path.exists(local_path):
            logger.info(f"Arquivo já existe: {local_path}")
            return True
            
        # Para ambiente Databricks, usamos dbutils para download
        dbutils.fs.cp(url, local_path)
        logger.info(f"Arquivo baixado: {url} -> {local_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao baixar {url}: {str(e)}")
        return False

def validate_data_quality(df, month_year: str) -> dict:
    """
    Validação básica de qualidade dos dados
    """
    total_rows = df.count()
    
    # Contagem de nulos nas colunas críticas
    critical_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_amount"]
    null_counts = {}
    
    for col in critical_columns:
        if col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            null_counts[col] = null_count
    
    # Validações adicionais
    negative_amounts = df.filter(col("total_amount") < 0).count()
    zero_distance = df.filter(col("trip_distance") == 0).count()
    
    quality_report = {
        "month_year": month_year,
        "total_rows": total_rows,
        "null_counts": null_counts,
        "negative_amounts": negative_amounts,
        "zero_distance_trips": zero_distance,
        "timestamp": datetime.now().isoformat()
    }
    
    return quality_report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestão dos Dados

# COMMAND ----------

# Criar database se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

print("Iniciando ingestão dos dados...")

# Lista para armazenar relatórios de qualidade
quality_reports = []

# Processar cada mês
for month_year, url in DATA_URLS.items():
    print(f"\nProcessando dados de {month_year}...")
    
    try:
        # Ler dados diretamente da URL (formato Parquet)
        df = spark.read.parquet(url)
        
        # Adicionar colunas de metadados
        df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp()) \
                            .withColumn("source_file", lit(f"yellow_tripdata_{month_year}.parquet")) \
                            .withColumn("ingestion_month", lit(month_year))
        
        # Validação de qualidade
        quality_report = validate_data_quality(df, month_year)
        quality_reports.append(quality_report)
        
        print(f"   {quality_report['total_rows']:,} registros carregados")
        print(f"   Nulos em colunas críticas: {quality_report['null_counts']}")
        
        # Escrever na camada Bronze (modo append para acumular os meses)
        if month_year == "2023-01":  # Primeiro mês - sobrescrever
            write_mode = "overwrite"
        else:  # Demais meses - adicionar
            write_mode = "append"
            
        df_with_metadata.write \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .partitionBy("ingestion_month") \
            .format("delta") \
            .saveAsTable(BRONZE_TABLE)
            
        print(f"   Dados salvos na tabela {BRONZE_TABLE}")
        
    except Exception as e:
        logger.error(f"Erro ao processar {month_year}: {str(e)}")
        continue

print("\nIngestão concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificação e Relatório Final

# COMMAND ----------

# Verificar tabela criada
bronze_df = spark.table(BRONZE_TABLE)

print("RELATÓRIO DE INGESTÃO - CAMADA BRONZE")
print("=" * 50)

# Estatísticas gerais
total_records = bronze_df.count()
print(f"Total de registros: {total_records:,}")

# Contagem por mês
monthly_counts = bronze_df.groupBy("ingestion_month").count().orderBy("ingestion_month")
print("\nRegistros por mês:")
monthly_counts.show()

# Período dos dados
date_range = bronze_df.agg(
    min("tpep_pickup_datetime").alias("min_date"),
    max("tpep_pickup_datetime").alias("max_date")
).collect()[0]

print(f"\n📆 Período dos dados:")
print(f"   Início: {date_range['min_date']}")
print(f"   Fim: {date_range['max_date']}")

# Schema da tabela
print(f"\nSchema da tabela:")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Relatório de Qualidade Detalhado

# COMMAND ----------

print("RELATÓRIO DE QUALIDADE DOS DADOS")
print("=" * 40)

for report in quality_reports:
    print(f"\n{report['month_year']}:")
    print(f"   Registros: {report['total_records']:,}")
    print(f"   Valores negativos em total_amount: {report['negative_amounts']:,}")
    print(f"   Viagens com distância zero: {report['zero_distance_trips']:,}")
    
    if report['null_counts']:
        print("   Valores nulos:")
        for col, null_count in report['null_counts'].items():
            percentage = (null_count / report['total_records']) * 100
            print(f"     {col}: {null_count:,} ({percentage:.2f}%)")

# Análise adicional de qualidade
print("\nANÁLISES ADICIONAIS:")

# Distribuição por vendor
vendor_dist = bronze_df.groupBy("VendorID").count().orderBy("VendorID")
print("\nDistribuição por Vendor:")
vendor_dist.show()

# Estatísticas de valores monetários
monetary_stats = bronze_df.select(
    avg("fare_amount").alias("avg_fare"),
    avg("tip_amount").alias("avg_tip"),
    avg("total_amount").alias("avg_total"),
    max("total_amount").alias("max_total"),
    min("total_amount").alias("min_total")
).collect()[0]

print("Estatísticas monetárias:")
print(f"   Tarifa média: ${monetary_stats['avg_fare']:.2f}")
print(f"   Gorjeta média: ${monetary_stats['avg_tip']:.2f}")
print(f"   Total médio: ${monetary_stats['avg_total']:.2f}")
print(f"   Total máximo: ${monetary_stats['max_total']:.2f}")
print(f"   Total mínimo: ${monetary_stats['min_total']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Otimização da Tabela Delta

# COMMAND ----------

# Otimizar tabela Delta
print("Otimizando tabela Delta...")

# OPTIMIZE e Z-ORDER
spark.sql(f"""
OPTIMIZE {BRONZE_TABLE}
ZORDER BY (tpep_pickup_datetime, VendorID)
""")

# Vacuum (limpar versões antigas - cuidado em produção)
# spark.sql(f"VACUUM {BRONZE_TABLE} RETAIN 168 HOURS")  # 7 dias

print("Otimização concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metadados e Documentação

# COMMAND ----------

# Informações sobre a tabela
print("INFORMAÇÕES DA TABELA BRONZE")
print("=" * 35)

# Mostrar informações da tabela
spark.sql(f"DESCRIBE EXTENDED {BRONZE_TABLE}").show(50, truncate=False)

# Histórico da tabela Delta
print("\n📚 Histórico da tabela Delta:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Camada Bronze
# MAGIC 
# MAGIC ### Dados Ingeridos:
# MAGIC - **Período:** Janeiro a Abril de 2023
# MAGIC - **Fonte:** NYC TLC Trip Record Data (formato Parquet)
# MAGIC - **Armazenamento:** Delta Lake particionado por mês de ingestão
# MAGIC - **Schema:** Preservado conforme dados originais
# MAGIC 
# MAGIC ### Controles de Qualidade:
# MAGIC - Validação de valores nulos em colunas críticas
# MAGIC - Identificação de valores negativos e inconsistências
# MAGIC - Relatório detalhado por mês de ingestão
# MAGIC - Metadados de ingestão adicionados
# MAGIC 
# MAGIC ### Próximos Passos:
# MAGIC 1. Executar notebook `02_tratamento_silver.py` para limpeza e tratamento
# MAGIC 2. Aplicar regras de negócio e validações adicionais
# MAGIC 3. Criar camada Silver com dados limpos e estruturados
# MAGIC 
# MAGIC ### Localização dos Dados:
# MAGIC - **Tabela:** `bronze.yellow_tripdata`
# MAGIC - **Particionamento:** `ingestion_month`
# MAGIC - **Formato:** Delta Lake com otimizações ZORDER

# COMMAND ----------

print("INGESTÃO BRONZE CONCLUÍDA COM SUCESSO!")
print("Próximo passo: Execute o notebook 02_tratamento_silver.py")
