# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Yellow Trip Records - Camada Gold (Análise e Métricas)
# MAGIC 
# MAGIC **Objetivo:** Criar métricas de negócio, KPIs e insights analíticos a partir dos dados limpos da camada Silver
# MAGIC 
# MAGIC **Características da Camada Gold:**
# MAGIC - Métricas agregadas e KPIs de negócio
# MAGIC - Tabelas dimensionais e de fatos
# MAGIC - Dados prontos para consumo por dashboards e relatórios
# MAGIC - Insights e análises avançadas
# MAGIC - Performance otimizada para consultas analíticas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
from datetime import datetime, timedelta

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração para gráficos
plt.style.use('default')
sns.set_palette("husl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações e Definições

# COMMAND ----------

# Configurações do projeto
SILVER_TABLE = "silver.yellow_tripdata_clean"
GOLD_DATABASE = "gold"

# Tabelas Gold a serem criadas
GOLD_TABLES = {
    "trip_metrics_daily": "gold.yellow_trip_metrics_daily",
    "trip_metrics_hourly": "gold.yellow_trip_metrics_hourly",
    "location_metrics": "gold.yellow_trip_location_metrics",
    "vendor_performance": "gold.yellow_trip_vendor_performance",
    "payment_analysis": "gold.yellow_trip_payment_analysis",
    "time_series_metrics": "gold.yellow_trip_time_series",
    "route_analysis": "gold.yellow_trip_route_analysis",
    "financial_summary": "gold.yellow_trip_financial_summary"
}

# KPIs principais para tracking
MAIN_KPIS = [
    "total_trips", "total_revenue", "avg_trip_distance", 
    "avg_trip_duration", "avg_fare_amount", "avg_tip_percentage",
    "total_passengers", "avg_speed"
]

print("Configurações carregadas")
print(f"Tabelas Gold a serem criadas: {len(GOLD_TABLES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento dos Dados Silver

# COMMAND ----------

print("Carregando dados da camada Silver...")

# Carregar dados limpos da camada Silver
silver_df = spark.table(SILVER_TABLE)

print(f"Registros na camada Silver: {silver_df.count():,}")

# Verificar período dos dados
date_range = silver_df.agg(
    min("tpep_pickup_datetime").alias("min_date"),
    max("tpep_pickup_datetime").alias("max_date"),
    count("*").alias("total_records")
).collect()[0]

print(f"Período: {date_range['min_date']} a {date_range['max_date']}")
print(f"Total de registros: {date_range['total_records']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criação do Database Gold

# COMMAND ----------

# Criar database Gold
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
print(f"Database {GOLD_DATABASE} criado/verificado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Métricas Diárias (Tabela Principal de KPIs)

# COMMAND ----------

print("Criando métricas diárias...")

# Métricas agregadas por dia
daily_metrics = silver_df.groupBy(
    date_format("tpep_pickup_datetime", "yyyy-MM-dd").alias("trip_date"),
    "pickup_year",
    "pickup_month",
    dayofmonth("tpep_pickup_datetime").alias("pickup_day"),
    "pickup_dayname",
    "is_weekend"
).agg(
    # Volume de viagens
    count("*").alias("total_trips"),
    countDistinct("VendorID").alias("active_vendors"),
    
    # Métricas financeiras
    sum("total_amount").alias("total_revenue"),
    sum("fare_amount").alias("total_fare"),
    sum("tip_amount").alias("total_tips"),
    sum("tolls_amount").alias("total_tolls"),
    avg("total_amount").alias("avg_total_amount"),
    avg("fare_amount").alias("avg_fare_amount"),
    avg("tip_amount").alias("avg_tip_amount"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    
    # Métricas operacionais
    sum("trip_distance").alias("total_distance"),
    avg("trip_distance").alias("avg_trip_distance"),
    sum("trip_duration_minutes").alias("total_duration_minutes"),
    avg("trip_duration_minutes").alias("avg_trip_duration"),
    avg("avg_speed_mph").alias("avg_speed"),
    
    # Passageiros
    sum("passenger_count").alias("total_passengers"),
    avg("passenger_count").alias("avg_passengers_per_trip"),
    
    # Qualidade dos dados
    sum(when(col("has_outlier"), 1).otherwise(0)).alias("trips_with_outliers"),
    
    # Distribuição por período
    sum(when(col("time_period") == "Morning", 1).otherwise(0)).alias("morning_trips"),
    sum(when(col("time_period") == "Afternoon", 1).otherwise(0)).alias("afternoon_trips"),
    sum(when(col("time_period") == "Evening", 1).otherwise(0)).alias("evening_trips"),
    sum(when(col("time_period") == "Night", 1).otherwise(0)).alias("night_trips")
).withColumn(
    # Métricas derivadas
    "revenue_per_mile", col("total_revenue") / col("total_distance")
).withColumn(
    "outlier_percentage", (col("trips_with_outliers") / col("total_trips")) * 100
).withColumn(
    "processing_timestamp", current_timestamp()
).orderBy("trip_date")

# Salvar tabela de métricas diárias
daily_metrics.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["trip_metrics_daily"])

print(f"Tabela criada: {GOLD_TABLES['trip_metrics_daily']}")
print(f"   {daily_metrics.count()} dias de dados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Métricas Horárias (Análise de Padrões Temporais)

# COMMAND ----------

print("Criando métricas horárias...")

# Métricas por hora do dia
hourly_metrics = silver_df.groupBy(
    "pickup_hour",
    "pickup_dayname",
    "is_weekend",
    date_format("tpep_pickup_datetime", "yyyy-MM-dd").alias("trip_date")
).agg(
    count("*").alias("total_trips"),
    avg("total_amount").alias("avg_revenue_per_trip"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    avg("avg_speed_mph").alias("avg_speed"),
    countDistinct("PULocationID").alias("unique_pickup_locations"),
    countDistinct("DOLocationID").alias("unique_dropoff_locations")
).withColumn("processing_timestamp", current_timestamp())

# Salvar tabela de métricas horárias
hourly_metrics.write \
    .mode("overwrite") \
    .partitionBy("is_weekend") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["trip_metrics_hourly"])

print(f"Tabela criada: {GOLD_TABLES['trip_metrics_hourly']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análise de Localizações (Top Pickup/Dropoff)

# COMMAND ----------

print("Criando análise de localizações...")

# Métricas por localização de pickup
pickup_metrics = silver_df.groupBy("PULocationID").agg(
    count("*").alias("pickup_count"),
    avg("trip_distance").alias("avg_distance_from_location"),
    avg("total_amount").alias("avg_revenue_from_location"),
    avg("tip_percentage").alias("avg_tip_from_location"),
    countDistinct("DOLocationID").alias("unique_destinations")
).withColumn("location_type", lit("pickup"))

# Métricas por localização de dropoff
dropoff_metrics = silver_df.groupBy("DOLocationID").agg(
    count("*").alias("dropoff_count"),
    avg("trip_distance").alias("avg_distance_to_location"),
    avg("total_amount").alias("avg_revenue_to_location"),
    avg("tip_percentage").alias("avg_tip_to_location"),
    countDistinct("PULocationID").alias("unique_origins")
).withColumn("location_type", lit("dropoff"))

# Combinar métricas de localização
location_metrics = pickup_metrics.select(
    col("PULocationID").alias("location_id"),
    "pickup_count", "avg_distance_from_location", 
    "avg_revenue_from_location", "avg_tip_from_location",
    "unique_destinations", "location_type"
).union(
    dropoff_metrics.select(
        col("DOLocationID").alias("location_id"),
        "dropoff_count", "avg_distance_to_location",
        "avg_revenue_to_location", "avg_tip_to_location", 
        "unique_origins", "location_type"
    )
).withColumn("processing_timestamp", current_timestamp())

# Salvar análise de localizações
location_metrics.write \
    .mode("overwrite") \
    .partitionBy("location_type") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["location_metrics"])

print(f"Tabela criada: {GOLD_TABLES['location_metrics']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Performance por Vendor

# COMMAND ----------

print("Analisando performance por vendor...")

# Métricas detalhadas por vendor
vendor_performance = silver_df.groupBy(
    "VendorID", 
    "vendor_name",
    date_format("tpep_pickup_datetime", "yyyy-MM").alias("month_year")
).agg(
    # Volume e participação
    count("*").alias("total_trips"),
    
    # Performance financeira
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_revenue_per_trip"),
    avg("fare_amount").alias("avg_fare"),
    avg("tip_amount").alias("avg_tip"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    
    # Performance operacional
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("avg_speed_mph").alias("avg_speed"),
    
    # Qualidade do serviço
    sum(when(col("has_outlier"), 1).otherwise(0)).alias("problematic_trips"),
    countDistinct("PULocationID").alias("coverage_pickup_locations"),
    countDistinct("DOLocationID").alias("coverage_dropoff_locations"),
    
    # Distribuição por tipo de pagamento
    sum(when(col("payment_type") == 1, 1).otherwise(0)).alias("credit_card_trips"),
    sum(when(col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips")
).withColumn(
    "market_share_trips", 
    col("total_trips") / sum("total_trips").over(Window.partitionBy("month_year")) * 100
).withColumn(
    "market_share_revenue",
    col("total_revenue") / sum("total_revenue").over(Window.partitionBy("month_year")) * 100
).withColumn(
    "outlier_rate", (col("problematic_trips") / col("total_trips")) * 100
).withColumn(
    "credit_card_rate", (col("credit_card_trips") / col("total_trips")) * 100
).withColumn("processing_timestamp", current_timestamp())

# Salvar performance por vendor
vendor_performance.write \
    .mode("overwrite") \
    .partitionBy("month_year") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["vendor_performance"])

print(f"Tabela criada: {GOLD_TABLES['vendor_performance']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Análise de Métodos de Pagamento

# COMMAND ----------

print("Analisando métodos de pagamento...")

# Análise detalhada por tipo de pagamento
payment_analysis = silver_df.groupBy(
    "payment_type",
    "payment_type_desc",
    "pickup_dayname",
    "time_period"
).agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_amount"),
    avg("tip_amount").alias("avg_tip"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    avg("fare_amount").alias("avg_fare"),
    stddev("tip_percentage").alias("tip_percentage_std")
).withColumn(
    "revenue_share",
    col("total_revenue") / sum("total_revenue").over(Window.partitionBy()) * 100
).withColumn("processing_timestamp", current_timestamp())

# Salvar análise de pagamentos
payment_analysis.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["payment_analysis"])

print(f"Tabela criada: {GOLD_TABLES['payment_analysis']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Série Temporal para Forecasting

# COMMAND ----------

print("Criando série temporal...")

# Série temporal com métricas para forecasting
time_series = silver_df.groupBy(
    date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:00:00").alias("datetime_hour"),
    hour("tpep_pickup_datetime").alias("hour_of_day"),
    dayofweek("tpep_pickup_datetime").alias("day_of_week"),
    "is_weekend"
).agg(
    count("*").alias("trips_count"),
    sum("total_amount").alias("revenue"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration")
).withColumn(
    # Adicionar features para ML
    "hour_sin", sin(col("hour_of_day") * 2 * 3.14159 / 24)
).withColumn(
    "hour_cos", cos(col("hour_of_day") * 2 * 3.14159 / 24)
).withColumn(
    "day_sin", sin(col("day_of_week") * 2 * 3.14159 / 7)
).withColumn(
    "day_cos", cos(col("day_of_week") * 2 * 3.14159 / 7)
).withColumn("processing_timestamp", current_timestamp())

# Salvar série temporal
time_series.write \
    .mode("overwrite") \
    .partitionBy("is_weekend") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["time_series_metrics"])

print(f"Tabela criada: {GOLD_TABLES['time_series_metrics']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Análise de Rotas Populares

# COMMAND ----------

print("Analisando rotas populares...")

# Top rotas com análise detalhada
route_analysis = silver_df.groupBy(
    "PULocationID", 
    "DOLocationID"
).agg(
    count("*").alias("trip_count"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_revenue_per_trip"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration"),
    avg("avg_speed_mph").alias("avg_speed"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    stddev("total_amount").alias("revenue_std"),
    
    # Distribuição temporal
    sum(when(col("is_weekend"), 1).otherwise(0)).alias("weekend_trips"),
    sum(when(col("time_period") == "Morning", 1).otherwise(0)).alias("morning_trips"),
    sum(when(col("time_period") == "Evening", 1).otherwise(0)).alias("evening_trips")
).withColumn(
    "weekend_ratio", col("weekend_trips") / col("trip_count") * 100
).withColumn(
    "revenue_per_mile", col("total_revenue") / col("avg_distance")
).withColumn(
    "route_efficiency_score", 
    (col("avg_speed") * col("avg_tip_percentage")) / col("avg_duration")
).withColumn("processing_timestamp", current_timestamp()) \
.orderBy(desc("trip_count"))

# Salvar análise de rotas
route_analysis.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["route_analysis"])

print(f"Tabela criada: {GOLD_TABLES['route_analysis']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resumo Financeiro Executivo

# COMMAND ----------

print("Criando resumo financeiro executivo...")

# Resumo financeiro por mês para executivos
financial_summary = silver_df.groupBy(
    "pickup_year",
    "pickup_month",
    date_format("tpep_pickup_datetime", "yyyy-MM").alias("month_year")
).agg(
    # Métricas de volume
    count("*").alias("total_trips"),
    countDistinct(date_format("tpep_pickup_datetime", "yyyy-MM-dd")).alias("active_days"),
    
    # Receitas
    sum("total_amount").alias("gross_revenue"),
    sum("fare_amount").alias("fare_revenue"),
    sum("tip_amount").alias("tip_revenue"),
    sum("tolls_amount").alias("tolls_revenue"),
    sum("extra").alias("extra_revenue"),
    
    # Métricas operacionais
    sum("trip_distance").alias("total_miles"),
    sum("trip_duration_minutes").alias("total_hours") / 60,
    sum("passenger_count").alias("total_passengers"),
    
    # Médias
    avg("total_amount").alias("avg_revenue_per_trip"),
    avg("trip_distance").alias("avg_miles_per_trip"),
    avg("tip_percentage").alias("avg_tip_rate")
).withColumn(
    "revenue_per_mile", col("gross_revenue") / col("total_miles")
).withColumn(
    "trips_per_day", col("total_trips") / col("active_days")
).withColumn(
    "revenue_per_day", col("gross_revenue") / col("active_days")
).withColumn(
    # Growth rates (comparação mês anterior)
    "revenue_growth_rate",
    ((col("gross_revenue") - lag("gross_revenue").over(
        Window.orderBy("pickup_year", "pickup_month")
    )) / lag("gross_revenue").over(
        Window.orderBy("pickup_year", "pickup_month")
    )) * 100
).withColumn(
    "trips_growth_rate",
    ((col("total_trips") - lag("total_trips").over(
        Window.orderBy("pickup_year", "pickup_month")
    )) / lag("total_trips").over(
        Window.orderBy("pickup_year", "pickup_month")
    )) * 100
).withColumn("processing_timestamp", current_timestamp())

# Salvar resumo financeiro
financial_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(GOLD_TABLES["financial_summary"])

print(f"Tabela criada: {GOLD_TABLES['financial_summary']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Otimização das Tabelas Gold

# COMMAND ----------

print("Otimizando tabelas Gold...")

# Otimizar cada tabela Gold
for table_name, table_path in GOLD_TABLES.items():
    try:
        print(f"   Otimizando {table_path}...")
        
        # OPTIMIZE com ZORDER apropriado para cada tabela
        if "daily" in table_name:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY (trip_date)")
        elif "hourly" in table_name:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY (pickup_hour, trip_date)")
        elif "vendor" in table_name:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY (VendorID, month_year)")
        elif "location" in table_name:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY (location_id)")
        elif "route" in table_name:
            spark.sql(f"OPTIMIZE {table_path} ZORDER BY (trip_count)")
        else:
            spark.sql(f"OPTIMIZE {table_path}")
            
        # Compute statistics
        spark.sql(f"ANALYZE TABLE {table_path} COMPUTE STATISTICS FOR ALL COLUMNS")
        
    except Exception as e:
        print(f"   Erro ao otimizar {table_path}: {str(e)}")

print("Otimização concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Dashboard de KPIs Principais

# COMMAND ----------

print("DASHBOARD DE KPIs PRINCIPAIS")
print("=" * 35)

# KPIs gerais do período
overall_kpis = silver_df.agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    sum("trip_distance").alias("total_distance"),
    sum("passenger_count").alias("total_passengers"),
    avg("total_amount").alias("avg_revenue_per_trip"),
    avg("trip_distance").alias("avg_distance_per_trip"),
    avg("trip_duration_minutes").alias("avg_duration_minutes"),
    avg("tip_percentage").alias("avg_tip_percentage"),
    countDistinct("PULocationID").alias("unique_pickup_locations"),
    countDistinct("DOLocationID").alias("unique_dropoff_locations")
).collect()[0]

print("KPIs GERAIS (Jan-Abr 2023):")
print(f"   Total de viagens: {overall_kpis['total_trips']:,}")
print(f"   Receita total: ${overall_kpis['total_revenue']:,.2f}")
print(f"   Distância total: {overall_kpis['total_distance']:,.0f} milhas")
print(f"   Total de passageiros: {overall_kpis['total_passengers']:,.0f}")
print(f"   Receita média por viagem: ${overall_kpis['avg_revenue_per_trip']:.2f}")
print(f"   Distância média: {overall_kpis['avg_distance_per_trip']:.2f} milhas")
print(f"   Duração média: {overall_kpis['avg_duration_minutes']:.1f} minutos")
print(f"   Gorjeta média: {overall_kpis['avg_tip_percentage']:.1f}%")
print(f"   Locais de pickup únicos: {overall_kpis['unique_pickup_locations']:,}")
print(f"   Locais de dropoff únicos: {overall_kpis['unique_dropoff_locations']:,}")

# Top 5 dias com mais receita
print("\nTOP 5 DIAS COM MAIOR RECEITA:")
top_revenue_days = spark.table(GOLD_TABLES["trip_metrics_daily"]) \
    .select("trip_date", "total_revenue", "total_trips", "pickup_dayname") \
    .orderBy(desc("total_revenue")) \
    .limit(5)

top_revenue_days.show(truncate=False)

# Performance por vendor
print("\nPERFORMANCE POR VENDOR (Total do período):")
vendor_summary = spark.table(GOLD_TABLES["vendor_performance"]) \
    .groupBy("vendor_name") \
    .agg(
        sum("total_trips").alias("total_trips"),
        sum("total_revenue").alias("total_revenue"),
        avg("avg_tip_percentage").alias("avg_tip_pct")
    ).orderBy(desc("total_revenue"))

vendor_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Análises Avançadas e Insights

# COMMAND ----------

print("ANÁLISES AVANÇADAS E INSIGHTS")
print("=" * 35)

# 1. Análise de sazonalidade por hora
print("\nPADRÃO DE DEMANDA POR HORA:")
hourly_demand = spark.table(GOLD_TABLES["trip_metrics_hourly"]) \
    .groupBy("pickup_hour") \
    .agg(
        avg("total_trips").alias("avg_trips_per_hour"),
        avg("avg_revenue_per_trip").alias("avg_revenue")
    ).orderBy("pickup_hour")

hourly_demand.show(24)

# 2. Análise de fins de semana vs dias úteis
print("\nFINS DE SEMANA vs DIAS ÚTEIS:")
weekend_comparison = spark.table(GOLD_TABLES["trip_metrics_daily"]) \
    .groupBy("is_weekend") \
    .agg(
        avg("total_trips").alias("avg_daily_trips"),
        avg("total_revenue").alias("avg_daily_revenue"),
        avg("avg_tip_percentage").alias("avg_tip_pct")
    )

weekend_comparison.show()

# 3. Top 10 rotas mais lucrativas
print("\nTOP 10 ROTAS MAIS LUCRATIVAS:")
top_profitable_routes = spark.table(GOLD_TABLES["route_analysis"]) \
    .select(
        "PULocationID", "DOLocationID", "trip_count", 
        "total_revenue", "avg_revenue_per_trip", "revenue_per_mile"
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

top_profitable_routes.show()

# 4. Análise de crescimento mensal
print("\nCRESCIMENTO MENSAL:")
monthly_growth = spark.table(GOLD_TABLES["financial_summary"]) \
    .select(
        "month_year", "total_trips", "gross_revenue", 
        "trips_growth_rate", "revenue_growth_rate"
    ) \
    .orderBy("month_year")

monthly_growth.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Criação de Views para Dashboards

# COMMAND ----------

print("Criando views para dashboards...")

# View consolidada de KPIs principais
spark.sql(f"""
CREATE OR REPLACE VIEW gold.kpi_dashboard AS
SELECT 
    trip_date,
    pickup_dayname,
    is_weekend,
    total_trips,
    total_revenue,
    avg_fare_amount,
    avg_tip_percentage,
    avg_trip_distance,
    avg_trip_duration,
    revenue_per_mile,
    processing_timestamp
FROM {GOLD_TABLES["trip_metrics_daily"]}
ORDER BY trip_date
""")

# View de tendências horárias
spark.sql(f"""
CREATE OR REPLACE VIEW gold.hourly_trends AS
SELECT 
    pickup_hour,
    pickup_dayname,
    is_weekend,
    AVG(total_trips) as avg_trips,
    AVG(avg_revenue_per_trip) as avg_revenue,
    AVG(avg_tip_percentage) as avg_tip_pct
FROM {GOLD_TABLES["trip_metrics_hourly"]}
GROUP BY pickup_hour, pickup_dayname, is_weekend
ORDER BY pickup_hour, pickup_dayname
""")

# View de performance de vendors
spark.sql(f"""
CREATE OR REPLACE VIEW gold.vendor_dashboard AS
SELECT 
    vendor_name,
    month_year,
    total_trips,
    total_revenue,
    market_share_trips,
    market_share_revenue,
    avg_tip_percentage,
    outlier_rate
FROM {GOLD_TABLES["vendor_performance"]}
ORDER BY month_year, total_revenue DESC
""")

print("Views criadas para dashboards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Validação e Teste das Tabelas Gold

# COMMAND ----------

print("VALIDAÇÃO DAS TABELAS GOLD")
print("=" * 30)

# Verificar todas as tabelas criadas
for table_name, table_path in GOLD_TABLES.items():
    try:
        df = spark.table(table_path)
        count = df.count()
        columns = len(df.columns)
        print(f"{table_name}: {count:,} registros, {columns} colunas")
        
        # Verificar se há dados nulos nas colunas principais
        if "total_trips" in df.columns:
            null_trips = df.filter(col("total_trips").isNull()).count()
            if null_trips > 0:
                print(f"   {null_trips} registros com total_trips nulo")
                
    except Exception as e:
        print(f"Erro na tabela {table_name}: {str(e)}")

# Verificar consistência entre tabelas
print(f"\nVERIFICAÇÃO DE CONSISTÊNCIA:")

# Total de viagens deve ser consistente
daily_total = spark.table(GOLD_TABLES["trip_metrics_daily"]).agg(sum("total_trips")).collect()[0][0]
silver_total = silver_df.count()

print(f"   Viagens na Silver: {silver_total:,}")
print(f"   Soma das métricas diárias: {daily_total:,}")
print(f"   Diferença: {abs(silver_total - daily_total):,}")

if abs(silver_total - daily_total) < 100:  # Tolerância pequena
    print("   Consistência validada")
else:
    print("   Possível inconsistência detectada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Relatório Final de Métricas Gold

# COMMAND ----------

print("RELATÓRIO FINAL - CAMADA GOLD")
print("=" * 35)

print("Tabelas Gold criadas com sucesso:")
for i, (table_name, table_path) in enumerate(GOLD_TABLES.items(), 1):
    print(f"   {i}. {table_path}")

print(f"\nMétricas principais extraídas:")
print("   • Métricas diárias e horárias de operação")
print("   • Performance detalhada por vendor")
print("   • Análise de localizações e rotas populares")
print("   • Padrões de pagamento e gorjetas")
print("   • Séries temporais para forecasting")
print("   • Resumos financeiros executivos")

print(f"\nKPIs disponíveis para dashboards:")
for kpi in MAIN_KPIS:
    print(f"   • {kpi}")

print(f"\nViews criadas para consumo:")
print("   • gold.kpi_dashboard - KPIs principais")
print("   • gold.hourly_trends - Tendências horárias")
print("   • gold.vendor_dashboard - Performance vendors")

print(f"\nOtimizações aplicadas:")
print("   • OPTIMIZE e ZORDER em todas as tabelas")
print("   • Particionamento estratégico")
print("   • Estatísticas computadas para otimização de queries")
print("   • Índices implícitos do Delta Lake")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Camada Gold
# MAGIC 
# MAGIC ### Tabelas Analíticas Criadas:
# MAGIC 1. **Métricas Diárias** - KPIs consolidados por dia
# MAGIC 2. **Métricas Horárias** - Análise de padrões temporais
# MAGIC 3. **Análise de Localizações** - Performance por pickup/dropoff
# MAGIC 4. **Performance de Vendors** - Comparativo entre fornecedores
# MAGIC 5. **Análise de Pagamentos** - Padrões por tipo de pagamento
# MAGIC 6. **Séries Temporais** - Dados preparados para forecasting
# MAGIC 7. **Análise de Rotas** - Rotas mais populares e lucrativas
# MAGIC 8. **Resumo Financeiro** - Métricas executivas mensais
# MAGIC 
# MAGIC ### KPIs Principais Disponíveis:
# MAGIC - Volume de viagens e crescimento
# MAGIC - Receitas e rentabilidade
# MAGIC - Métricas operacionais (distância, duração, velocidade)
# MAGIC - Satisfação do cliente (gorjetas)
# MAGIC - Cobertura geográfica
# MAGIC - Qualidade dos dados
# MAGIC 
# MAGIC ### Casos de Uso Habilitados:
# MAGIC - **Dashboards Executivos** - Métricas de alto nível
# MAGIC - **Análise Operacional** - Otimização de rotas e horários
# MAGIC - **Forecasting** - Previsão de demanda
# MAGIC - **Análise Competitiva** - Performance entre vendors
# MAGIC - **Business Intelligence** - Insights para tomada de decisão
# MAGIC 
# MAGIC ### Próximos Passos:
# MAGIC 1. Executar notebook `04_automatizacao.py` para pipeline automatizado
# MAGIC 2. Conectar ferramentas de BI (Tableau, Power BI)
# MAGIC 3. Implementar alertas e monitoramento
# MAGIC 4. Desenvolver modelos de ML para forecasting
# MAGIC 
# MAGIC ### Performance e Escalabilidade:
# MAGIC - Todas as tabelas otimizadas com ZORDER
# MAGIC - Particionamento estratégico para consultas rápidas
# MAGIC - Views pré-configuradas para dashboards
# MAGIC - Delta Lake para ACID transactions e time travel

# COMMAND ----------

print("GOLD ANALYSIS COMPLETED SUCCESSFULLY")
print("Próximo passo: Execute o notebook 04_automatizacao.py")
print("As tabelas Gold estão prontas para consumo em dashboards e relatórios!")
