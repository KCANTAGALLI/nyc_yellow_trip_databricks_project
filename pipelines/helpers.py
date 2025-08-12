"""
NYC Yellow Trip Records - Utility Functions
===========================================

Production-ready utility functions for the NYC Yellow Trip data pipeline.
Provides data validation, quality metrics, and processing functions
for Bronze, Silver, and Gold layers.

Version: 1.0
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple, Union
import logging
import json
from datetime import datetime, timedelta
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# CONSTANTES E CONFIGURAÇÕES
# ====================================

# Schema padrão dos dados Yellow Trip
YELLOW_TRIP_SCHEMA = StructType([
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

# Mapeamentos de códigos
CODE_MAPPINGS = {
    "vendor": {
        1: "Creative Mobile Technologies",
        2: "VeriFone Inc"
    },
    "ratecode": {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    },
    "payment_type": {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
}

# Limites de validação padrão
DEFAULT_VALIDATION_RULES = {
    "fare_amount": {"min": 0.0, "max": 1000.0},
    "tip_amount": {"min": 0.0, "max": 500.0},
    "total_amount": {"min": 0.0, "max": 1500.0},
    "trip_distance": {"min": 0.0, "max": 500.0},
    "duration_minutes": {"min": 1, "max": 1440},  # 1 min a 24h
    "passenger_count": {"min": 0, "max": 6},
    "max_speed_mph": 200
}

# ====================================
# FUNÇÕES DE VALIDAÇÃO E LIMPEZA
# ====================================

def validate_dataframe_schema(df: DataFrame, expected_schema: StructType = YELLOW_TRIP_SCHEMA) -> Tuple[bool, List[str]]:
    """
    Valida se o DataFrame possui o schema esperado.
    
    Args:
        df: DataFrame a ser validado
        expected_schema: Schema esperado
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, missing_columns)
    """
    try:
        df_columns = set(df.columns)
        expected_columns = set([field.name for field in expected_schema.fields])
        
        missing_columns = expected_columns - df_columns
        extra_columns = df_columns - expected_columns
        
        is_valid = len(missing_columns) == 0
        
        issues = []
        if missing_columns:
            issues.extend([f"Coluna ausente: {col}" for col in missing_columns])
        if extra_columns:
            issues.extend([f"Coluna extra: {col}" for col in extra_columns])
            
        logger.info(f"Validação de schema - Válido: {is_valid}, Issues: {len(issues)}")
        return is_valid, issues
        
    except Exception as e:
        logger.error(f"Erro na validação de schema: {str(e)}")
        return False, [f"Erro na validação: {str(e)}"]

def apply_data_quality_filters(df: DataFrame, rules: Dict = None) -> Tuple[DataFrame, Dict]:
    """
    Aplica filtros de qualidade de dados baseado em regras definidas.
    
    Args:
        df: DataFrame a ser filtrado
        rules: Regras de validação customizadas
        
    Returns:
        Tuple[DataFrame, Dict]: (filtered_df, quality_report)
    """
    if rules is None:
        rules = DEFAULT_VALIDATION_RULES
    
    try:
        original_count = df.count()
        
        # Adicionar duração da viagem para validação
        df_with_duration = df.withColumn(
            "trip_duration_minutes",
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
        )
        
        # Aplicar filtros
        df_filtered = df_with_duration.filter(
            # Timestamps válidos
            (col("tpep_pickup_datetime").isNotNull()) &
            (col("tpep_dropoff_datetime").isNotNull()) &
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
            
            # Valores monetários
            (col("fare_amount") >= rules["fare_amount"]["min"]) &
            (col("fare_amount") <= rules["fare_amount"]["max"]) &
            (col("tip_amount") >= rules["tip_amount"]["min"]) &
            (col("tip_amount") <= rules["tip_amount"]["max"]) &
            (col("total_amount") >= rules["total_amount"]["min"]) &
            (col("total_amount") <= rules["total_amount"]["max"]) &
            
            # Distância e duração
            (col("trip_distance") >= rules["trip_distance"]["min"]) &
            (col("trip_distance") <= rules["trip_distance"]["max"]) &
            (col("trip_duration_minutes") >= rules["duration_minutes"]["min"]) &
            (col("trip_duration_minutes") <= rules["duration_minutes"]["max"]) &
            
            # Passageiros
            (col("passenger_count") >= rules["passenger_count"]["min"]) &
            (col("passenger_count") <= rules["passenger_count"]["max"]) &
            
            # IDs válidos
            (col("PULocationID").isNotNull()) &
            (col("DOLocationID").isNotNull()) &
            (col("PULocationID") > 0) &
            (col("DOLocationID") > 0) &
            (col("VendorID").isin([1, 2]))
        )
        
        filtered_count = df_filtered.count()
        removed_count = original_count - filtered_count
        
        quality_report = {
            "original_records": original_count,
            "filtered_records": filtered_count,
            "removed_records": removed_count,
            "removal_rate": (removed_count / original_count * 100) if original_count > 0 else 0,
            "quality_score": (filtered_count / original_count) if original_count > 0 else 0
        }
        
        logger.info(f"Filtros aplicados - Removidos: {removed_count} ({quality_report['removal_rate']:.2f}%)")
        
        return df_filtered.drop("trip_duration_minutes"), quality_report
        
    except Exception as e:
        logger.error(f"Erro ao aplicar filtros de qualidade: {str(e)}")
        return df, {"error": str(e)}

def detect_outliers_iqr(df: DataFrame, columns: List[str], iqr_multiplier: float = 1.5) -> DataFrame:
    """
    Detecta outliers usando método IQR (Interquartile Range).
    
    Args:
        df: DataFrame a ser analisado
        columns: Lista de colunas para análise
        iqr_multiplier: Multiplicador para definir limites (padrão 1.5)
        
    Returns:
        DataFrame com flags de outliers adicionadas
    """
    try:
        df_result = df
        
        for col_name in columns:
            if col_name in df.columns:
                # Calcular quartis
                quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
                
                if len(quantiles) == 2:
                    q1, q3 = quantiles
                    iqr = q3 - q1
                    lower_bound = q1 - iqr_multiplier * iqr
                    upper_bound = q3 + iqr_multiplier * iqr
                    
                    # Adicionar flag de outlier
                    df_result = df_result.withColumn(
                        f"{col_name}_outlier",
                        when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), True)
                        .otherwise(False)
                    )
                    
                    logger.info(f"Outliers detectados para {col_name}: bounds [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        # Flag geral de outlier
        outlier_columns = [f"{col}_outlier" for col in columns if f"{col}_outlier" in df_result.columns]
        if outlier_columns:
            df_result = df_result.withColumn(
                "has_outlier",
                expr(" OR ".join(outlier_columns))
            )
        
        return df_result
        
    except Exception as e:
        logger.error(f"Erro na detecção de outliers: {str(e)}")
        return df

# ====================================
# FUNÇÕES DE ENRIQUECIMENTO
# ====================================

def add_temporal_features(df: DataFrame, datetime_col: str = "tpep_pickup_datetime") -> DataFrame:
    """
    Adiciona features temporais derivadas da coluna de datetime.
    
    Args:
        df: DataFrame a ser enriquecido
        datetime_col: Nome da coluna de datetime
        
    Returns:
        DataFrame com features temporais adicionadas
    """
    try:
        df_enriched = df.withColumn(
            "pickup_hour", hour(datetime_col)
        ).withColumn(
            "pickup_day", dayofmonth(datetime_col)
        ).withColumn(
            "pickup_month", month(datetime_col)
        ).withColumn(
            "pickup_year", year(datetime_col)
        ).withColumn(
            "pickup_dayofweek", dayofweek(datetime_col)
        ).withColumn(
            "pickup_dayname",
            when(dayofweek(datetime_col) == 1, "Sunday")
            .when(dayofweek(datetime_col) == 2, "Monday")
            .when(dayofweek(datetime_col) == 3, "Tuesday")
            .when(dayofweek(datetime_col) == 4, "Wednesday")
            .when(dayofweek(datetime_col) == 5, "Thursday")
            .when(dayofweek(datetime_col) == 6, "Friday")
            .when(dayofweek(datetime_col) == 7, "Saturday")
        ).withColumn(
            "time_period",
            when(col("pickup_hour").between(6, 11), "Morning")
            .when(col("pickup_hour").between(12, 17), "Afternoon")
            .when(col("pickup_hour").between(18, 21), "Evening")
            .otherwise("Night")
        ).withColumn(
            "is_weekend",
            when(col("pickup_dayofweek").isin([1, 7]), True).otherwise(False)
        ).withColumn(
            "is_business_hour",
            when(col("pickup_hour").between(9, 17), True).otherwise(False)
        )
        
        logger.info("Features temporais adicionadas com sucesso")
        return df_enriched
        
    except Exception as e:
        logger.error(f"Erro ao adicionar features temporais: {str(e)}")
        return df

def add_trip_metrics(df: DataFrame) -> DataFrame:
    """
    Adiciona métricas calculadas da viagem.
    
    Args:
        df: DataFrame com dados de viagem
        
    Returns:
        DataFrame com métricas adicionadas
    """
    try:
        df_metrics = df.withColumn(
            # Duração da viagem
            "trip_duration_minutes",
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
        ).withColumn(
            # Velocidade média
            "avg_speed_mph",
            when(col("trip_duration_minutes") > 0,
                 col("trip_distance") / (col("trip_duration_minutes") / 60))
            .otherwise(0)
        ).withColumn(
            # Percentual de gorjeta
            "tip_percentage",
            when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
            .otherwise(0)
        ).withColumn(
            # Custo por milha
            "cost_per_mile",
            when(col("trip_distance") > 0, col("total_amount") / col("trip_distance"))
            .otherwise(0)
        ).withColumn(
            # Eficiência da viagem (receita por minuto)
            "revenue_per_minute",
            when(col("trip_duration_minutes") > 0, col("total_amount") / col("trip_duration_minutes"))
            .otherwise(0)
        )
        
        logger.info("Métricas de viagem adicionadas com sucesso")
        return df_metrics
        
    except Exception as e:
        logger.error(f"Erro ao adicionar métricas de viagem: {str(e)}")
        return df

def add_business_mappings(df: DataFrame, spark_session: SparkSession) -> DataFrame:
    """
    Adiciona mapeamentos de códigos de negócio.
    
    Args:
        df: DataFrame a ser enriquecido
        spark_session: Sessão Spark
        
    Returns:
        DataFrame com mapeamentos adicionados
    """
    try:
        # Criar DataFrames de mapeamento
        vendor_df = spark_session.createDataFrame(
            [(k, v) for k, v in CODE_MAPPINGS["vendor"].items()],
            ["VendorID", "vendor_name"]
        )
        
        ratecode_df = spark_session.createDataFrame(
            [(k, v) for k, v in CODE_MAPPINGS["ratecode"].items()],
            ["RatecodeID", "rate_code_desc"]
        )
        
        payment_df = spark_session.createDataFrame(
            [(k, v) for k, v in CODE_MAPPINGS["payment_type"].items()],
            ["payment_type", "payment_type_desc"]
        )
        
        # Aplicar joins
        df_mapped = df.join(vendor_df, "VendorID", "left") \
                     .join(ratecode_df, df.RatecodeID == ratecode_df.RatecodeID, "left") \
                     .join(payment_df, "payment_type", "left") \
                     .drop(ratecode_df.RatecodeID)
        
        logger.info("Mapeamentos de negócio adicionados com sucesso")
        return df_mapped
        
    except Exception as e:
        logger.error(f"Erro ao adicionar mapeamentos: {str(e)}")
        return df

# ====================================
# FUNÇÕES DE ANÁLISE E MÉTRICAS
# ====================================

def calculate_data_quality_metrics(df: DataFrame) -> Dict:
    """
    Calcula métricas de qualidade dos dados.
    
    Args:
        df: DataFrame a ser analisado
        
    Returns:
        Dict com métricas de qualidade
    """
    try:
        total_records = df.count()
        
        if total_records == 0:
            return {"total_records": 0, "quality_score": 0}
        
        # Colunas críticas para análise
        critical_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", 
                          "trip_distance", "total_amount", "PULocationID", "DOLocationID"]
        
        metrics = {
            "total_records": total_records,
            "null_counts": {},
            "null_percentages": {},
            "duplicate_records": 0,
            "negative_amounts": 0,
            "zero_distance_trips": 0,
            "future_trips": 0
        }
        
        # Contar nulos por coluna
        for col_name in critical_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                metrics["null_counts"][col_name] = null_count
                metrics["null_percentages"][col_name] = (null_count / total_records) * 100
        
        # Outras validações
        metrics["duplicate_records"] = df.count() - df.dropDuplicates().count()
        metrics["negative_amounts"] = df.filter(col("total_amount") < 0).count()
        metrics["zero_distance_trips"] = df.filter(col("trip_distance") == 0).count()
        
        # Viagens no futuro
        current_timestamp = datetime.now()
        metrics["future_trips"] = df.filter(col("tpep_pickup_datetime") > current_timestamp).count()
        
        # Calcular score geral de qualidade
        total_issues = (
            sum(metrics["null_counts"].values()) +
            metrics["duplicate_records"] +
            metrics["negative_amounts"] +
            metrics["future_trips"]
        )
        
        metrics["quality_score"] = max(0, 1 - (total_issues / total_records))
        
        logger.info(f"Métricas de qualidade calculadas - Score: {metrics['quality_score']:.3f}")
        return metrics
        
    except Exception as e:
        logger.error(f"Erro ao calcular métricas de qualidade: {str(e)}")
        return {"error": str(e)}

def generate_summary_statistics(df: DataFrame) -> Dict:
    """
    Gera estatísticas resumidas do dataset.
    
    Args:
        df: DataFrame a ser analisado
        
    Returns:
        Dict com estatísticas resumidas
    """
    try:
        # Estatísticas básicas
        total_records = df.count()
        
        if total_records == 0:
            return {"total_records": 0}
        
        # Período dos dados
        date_stats = df.agg(
            min("tpep_pickup_datetime").alias("min_date"),
            max("tpep_pickup_datetime").alias("max_date")
        ).collect()[0]
        
        # Estatísticas financeiras
        financial_stats = df.agg(
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_revenue"),
            sum("fare_amount").alias("total_fare"),
            avg("fare_amount").alias("avg_fare"),
            sum("tip_amount").alias("total_tips"),
            avg("tip_amount").alias("avg_tip")
        ).collect()[0]
        
        # Estatísticas operacionais
        operational_stats = df.agg(
            sum("trip_distance").alias("total_distance"),
            avg("trip_distance").alias("avg_distance"),
            sum("passenger_count").alias("total_passengers"),
            avg("passenger_count").alias("avg_passengers"),
            countDistinct("PULocationID").alias("unique_pickup_locations"),
            countDistinct("DOLocationID").alias("unique_dropoff_locations"),
            countDistinct("VendorID").alias("unique_vendors")
        ).collect()[0]
        
        summary = {
            "total_records": total_records,
            "date_range": {
                "start": date_stats["min_date"],
                "end": date_stats["max_date"],
                "days": (date_stats["max_date"] - date_stats["min_date"]).days if date_stats["min_date"] and date_stats["max_date"] else 0
            },
            "financial": {
                "total_revenue": float(financial_stats["total_revenue"] or 0),
                "avg_revenue": float(financial_stats["avg_revenue"] or 0),
                "total_fare": float(financial_stats["total_fare"] or 0),
                "avg_fare": float(financial_stats["avg_fare"] or 0),
                "total_tips": float(financial_stats["total_tips"] or 0),
                "avg_tip": float(financial_stats["avg_tip"] or 0)
            },
            "operational": {
                "total_distance": float(operational_stats["total_distance"] or 0),
                "avg_distance": float(operational_stats["avg_distance"] or 0),
                "total_passengers": int(operational_stats["total_passengers"] or 0),
                "avg_passengers": float(operational_stats["avg_passengers"] or 0),
                "unique_pickup_locations": operational_stats["unique_pickup_locations"],
                "unique_dropoff_locations": operational_stats["unique_dropoff_locations"],
                "unique_vendors": operational_stats["unique_vendors"]
            }
        }
        
        logger.info("Estatísticas resumidas geradas com sucesso")
        return summary
        
    except Exception as e:
        logger.error(f"Erro ao gerar estatísticas: {str(e)}")
        return {"error": str(e)}

# ====================================
# FUNÇÕES DE UTILIDADE
# ====================================

def save_dataframe_with_metadata(df: DataFrame, table_name: str, mode: str = "overwrite", 
                                partition_cols: List[str] = None, processing_id: str = None) -> bool:
    """
    Salva DataFrame com metadados de processamento.
    
    Args:
        df: DataFrame a ser salvo
        table_name: Nome da tabela
        mode: Modo de escrita ("overwrite", "append")
        partition_cols: Colunas para particionamento
        processing_id: ID de processamento único
        
    Returns:
        bool: Sucesso da operação
    """
    try:
        # Adicionar metadados
        df_with_metadata = df.withColumn("processing_timestamp", current_timestamp())
        
        if processing_id:
            df_with_metadata = df_with_metadata.withColumn("processing_id", lit(processing_id))
        
        # Configurar escrita
        writer = df_with_metadata.write.mode(mode).format("delta")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        # Salvar
        writer.saveAsTable(table_name)
        
        logger.info(f"DataFrame salvo com sucesso: {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar DataFrame: {str(e)}")
        return False

def optimize_delta_table(table_name: str, zorder_cols: List[str] = None, 
                        vacuum_hours: int = None) -> bool:
    """
    Otimiza tabela Delta com OPTIMIZE e opcionalmente VACUUM.
    
    Args:
        table_name: Nome da tabela
        zorder_cols: Colunas para ZORDER
        vacuum_hours: Horas para retenção no VACUUM
        
    Returns:
        bool: Sucesso da operação
    """
    try:
        spark = SparkSession.getActiveSession()
        
        # OPTIMIZE
        optimize_sql = f"OPTIMIZE {table_name}"
        if zorder_cols:
            optimize_sql += f" ZORDER BY ({', '.join(zorder_cols)})"
        
        spark.sql(optimize_sql)
        logger.info(f"OPTIMIZE executado para {table_name}")
        
        # VACUUM se especificado
        if vacuum_hours is not None:
            spark.sql(f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS")
            logger.info(f"VACUUM executado para {table_name}")
        
        # Compute statistics
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
        logger.info(f"Estatísticas computadas para {table_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao otimizar tabela: {str(e)}")
        return False

def create_data_quality_report(df: DataFrame, report_name: str = "Data Quality Report") -> str:
    """
    Cria relatório de qualidade dos dados em formato texto.
    
    Args:
        df: DataFrame a ser analisado
        report_name: Nome do relatório
        
    Returns:
        str: Relatório formatado
    """
    try:
        metrics = calculate_data_quality_metrics(df)
        summary = generate_summary_statistics(df)
        
        report = f"""
{report_name}
{'=' * len(report_name)}

RESUMO GERAL:
- Total de registros: {summary.get('total_records', 0):,}
- Período: {summary.get('date_range', {}).get('start')} a {summary.get('date_range', {}).get('end')}
- Score de qualidade: {metrics.get('quality_score', 0):.3f}

MÉTRICAS FINANCEIRAS:
- Receita total: ${summary.get('financial', {}).get('total_revenue', 0):,.2f}
- Receita média por viagem: ${summary.get('financial', {}).get('avg_revenue', 0):.2f}
- Gorjetas totais: ${summary.get('financial', {}).get('total_tips', 0):,.2f}

MÉTRICAS OPERACIONAIS:
- Distância total: {summary.get('operational', {}).get('total_distance', 0):,.1f} milhas
- Distância média: {summary.get('operational', {}).get('avg_distance', 0):.2f} milhas
- Passageiros totais: {summary.get('operational', {}).get('total_passengers', 0):,}

QUALIDADE DOS DADOS:
- Registros duplicados: {metrics.get('duplicate_records', 0):,}
- Valores negativos: {metrics.get('negative_amounts', 0):,}
- Viagens com distância zero: {metrics.get('zero_distance_trips', 0):,}
- Viagens no futuro: {metrics.get('future_trips', 0):,}

VALORES NULOS POR COLUNA:
"""
        
        for col, percentage in metrics.get('null_percentages', {}).items():
            report += f"- {col}: {percentage:.2f}%\n"
        
        report += f"\nRelatório gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return report
        
    except Exception as e:
        logger.error(f"Erro ao criar relatório: {str(e)}")
        return f"Erro ao gerar relatório: {str(e)}"

def validate_business_rules(df: DataFrame) -> Dict:
    """
    Valida regras específicas de negócio para dados de táxi.
    
    Args:
        df: DataFrame a ser validado
        
    Returns:
        Dict com resultados da validação
    """
    try:
        total_records = df.count()
        
        if total_records == 0:
            return {"total_records": 0, "all_rules_passed": False}
        
        validations = {}
        
        # Regra 1: Pickup deve ser antes de dropoff
        validations["valid_trip_sequence"] = df.filter(
            col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")
        ).count()
        
        # Regra 2: Valores monetários consistentes
        validations["consistent_amounts"] = df.filter(
            col("total_amount") >= (col("fare_amount") + col("extra") + col("mta_tax") + col("improvement_surcharge") - 1)  # tolerância de $1
        ).count()
        
        # Regra 3: Gorjeta apenas para cartão de crédito (principalmente)
        credit_card_trips = df.filter(col("payment_type") == 1).count()
        tips_with_credit = df.filter((col("payment_type") == 1) & (col("tip_amount") > 0)).count()
        
        validations["credit_card_trips"] = credit_card_trips
        validations["tips_with_credit"] = tips_with_credit
        
        # Regra 4: Distância e duração fazem sentido (velocidade razoável)
        df_with_speed = df.withColumn(
            "speed_mph",
            when(col("trip_duration_minutes") > 0,
                 col("trip_distance") / (col("trip_duration_minutes") / 60))
            .otherwise(0)
        )
        
        validations["reasonable_speed"] = df_with_speed.filter(
            (col("speed_mph") >= 0.5) & (col("speed_mph") <= 80)  # Entre 0.5 e 80 mph
        ).count()
        
        # Regra 5: IDs de localização válidos (assumindo NYC tem IDs 1-263)
        validations["valid_location_ids"] = df.filter(
            (col("PULocationID").between(1, 263)) & 
            (col("DOLocationID").between(1, 263))
        ).count()
        
        # Calcular percentuais
        results = {
            "total_records": total_records,
            "validations": {}
        }
        
        for rule, count in validations.items():
            results["validations"][rule] = {
                "valid_count": count,
                "invalid_count": total_records - count,
                "valid_percentage": (count / total_records) * 100
            }
        
        # Verificar se todas as regras passaram (>95% válido)
        all_passed = all(
            v["valid_percentage"] > 95 for v in results["validations"].values()
        )
        results["all_rules_passed"] = all_passed
        
        logger.info(f"Validação de regras de negócio - Todas passaram: {all_passed}")
        return results
        
    except Exception as e:
        logger.error(f"Erro na validação de regras de negócio: {str(e)}")
        return {"error": str(e)}

# ====================================
# FUNÇÕES DE CONFIGURAÇÃO
# ====================================

def get_spark_session(app_name: str = "NYC_Taxi_ETL", 
                     config: Dict = None) -> SparkSession:
    """
    Cria ou obtém sessão Spark com configurações otimizadas.
    
    Args:
        app_name: Nome da aplicação
        config: Configurações adicionais do Spark
        
    Returns:
        SparkSession configurada
    """
    try:
        builder = SparkSession.builder.appName(app_name)
        
        # Configurações padrão
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.databricks.delta.preview.enabled": "true"
        }
        
        # Aplicar configurações
        final_config = {**default_config, **(config or {})}
        
        for key, value in final_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Configurar nível de log
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Sessão Spark criada: {app_name}")
        return spark
        
    except Exception as e:
        logger.error(f"Erro ao criar sessão Spark: {str(e)}")
        raise

# ====================================
# EXEMPLO DE USO
# ====================================

if __name__ == "__main__":
    """
    Exemplo de uso das funções auxiliares
    """
    
    # Criar sessão Spark
    spark = get_spark_session("NYC_Taxi_Helpers_Test")
    
    # Exemplo de DataFrame (substituir por dados reais)
    sample_data = [
        (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1.0, 5.2, 1.0, "N", 100, 200, 1, 15.0, 0.5, 0.5, 3.0, 0.0, 0.3, 19.3, 2.5, 0.0),
        (2, "2023-01-01 11:00:00", "2023-01-01 11:25:00", 2.0, 3.1, 1.0, "N", 150, 250, 1, 12.5, 0.5, 0.5, 2.5, 0.0, 0.3, 16.3, 2.5, 0.0)
    ]
    
    sample_df = spark.createDataFrame(sample_data, YELLOW_TRIP_SCHEMA)
    
    # Testar funções
    print("=== TESTE DAS FUNÇÕES AUXILIARES ===")
    
    # 1. Validar schema
    is_valid, issues = validate_dataframe_schema(sample_df)
    print(f"Schema válido: {is_valid}")
    
    # 2. Adicionar features temporais
    df_with_temporal = add_temporal_features(sample_df)
    print(f"Colunas após features temporais: {len(df_with_temporal.columns)}")
    
    # 3. Adicionar métricas
    df_with_metrics = add_trip_metrics(df_with_temporal)
    print(f"Colunas após métricas: {len(df_with_metrics.columns)}")
    
    # 4. Gerar relatório de qualidade
    report = create_data_quality_report(df_with_metrics, "Teste de Qualidade")
    print("\n" + report)
    
    print("\nTeste das funções auxiliares concluído!")
    
    spark.stop()
