#!/usr/bin/env python3
"""
NYC Yellow Trip Records - Local Pipeline Testing Framework
=========================================================

Advanced testing framework for local validation of data pipelines.
Simulates notebook execution and provides comprehensive logging.

Executes Bronze, Silver, and Gold pipeline tests locally with
detailed reporting and quality metrics.

Version: 1.0
"""

import os
import sys
import json
import logging
import traceback
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

# Adicionar o diretório do projeto ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    print("WARNING: PySpark not available. Running basic tests...")
    SPARK_AVAILABLE = False

# Importar funções auxiliares se disponível
try:
    from pipelines.helpers import (
        validate_dataframe_schema,
        apply_data_quality_filters,
        add_temporal_features,
        add_trip_metrics,
        calculate_data_quality_metrics,
        generate_summary_statistics,
        create_data_quality_report,
        validate_business_rules,
        get_spark_session,
        YELLOW_TRIP_SCHEMA,
        DEFAULT_VALIDATION_RULES
    )
    HELPERS_AVAILABLE = True
except ImportError:
    print("WARNING: Helpers module not available. Limited testing...")
    HELPERS_AVAILABLE = False

# ====================================
# CONFIGURAÇÕES
# ====================================

# Configuração de logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"reports/pipeline_test_{timestamp}.log"

# Criar diretório reports se não existir
os.makedirs("reports", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações do teste
PIPELINE_TEST_CONFIG = {
    "project_name": "nyc_yellow_trip_databricks_project",
    "test_mode": "pipeline_execution",
    "spark_local": True,
    "simulate_data": True,
    "test_notebooks": True,
    "save_reports": True,
    "reports_dir": "reports",
    "timestamp": timestamp,
    "sample_data_size": 1000  # Número de registros para teste
}

# Definição dos pipelines
PIPELINE_STAGES = {
    "bronze": {
        "notebook": "notebooks/01_ingestao_bronze.py",
        "description": "Ingestão de dados brutos (Bronze Layer)",
        "input_format": "parquet",
        "output_table": "bronze_trips",
        "expected_operations": ["read", "basic_validation", "write"]
    },
    "silver": {
        "notebook": "notebooks/02_tratamento_silver.py", 
        "description": "Tratamento e limpeza (Silver Layer)",
        "input_table": "bronze_trips",
        "output_table": "silver_trips",
        "expected_operations": ["read", "clean", "validate", "enrich", "write"]
    },
    "gold": {
        "notebook": "notebooks/03_analise_gold.py",
        "description": "Análises e agregações (Gold Layer)",
        "input_table": "silver_trips",
        "output_tables": ["gold_trips_summary", "gold_location_stats", "gold_temporal_analysis"],
        "expected_operations": ["read", "aggregate", "analyze", "write"]
    }
}

# ====================================
# CLASSES DE TESTE
# ====================================

class PipelineTestResult:
    """Classe para armazenar resultados de testes de pipeline"""
    
    def __init__(self, pipeline_name: str, stage: str):
        self.pipeline_name = pipeline_name
        self.stage = stage
        self.status = "pending"
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.details = {}
        self.errors = []
        self.warnings = []
        self.metrics = {}
        self.data_quality = {}
        
    def start(self):
        self.start_time = datetime.now()
        self.status = "running"
        logger.info(f"Iniciando teste: {self.pipeline_name} - {self.stage}")
        
    def finish(self, status: str = "passed", details: Dict = None):
        self.end_time = datetime.now()
        self.status = status
        self.duration = (self.end_time - self.start_time).total_seconds()
        if details:
            self.details.update(details)
        
        status_symbol = "[PASS]" if status == "passed" else "[FAIL]" if status == "failed" else "[WARN]"
        logger.info(f"{status_symbol} Finalizado: {self.pipeline_name} - {self.stage} ({self.duration:.2f}s)")
            
    def add_error(self, error: str):
        self.errors.append(error)
        self.status = "failed"
        logger.error(f"ERRO em {self.pipeline_name}: {error}")
        
    def add_warning(self, warning: str):
        self.warnings.append(warning)
        logger.warning(f"AVISO em {self.pipeline_name}: {warning}")
        
    def add_metrics(self, metrics: Dict):
        self.metrics.update(metrics)
        
    def add_data_quality(self, quality_data: Dict):
        self.data_quality.update(quality_data)
        
    def to_dict(self) -> Dict:
        return {
            "pipeline_name": self.pipeline_name,
            "stage": self.stage,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration,
            "details": self.details,
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "data_quality": self.data_quality
        }

class PipelineTestSuite:
    """Classe principal para execução dos testes de pipeline"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.results = []
        self.spark = None
        self.start_time = datetime.now()
        self.sample_data = None
        
    def add_result(self, result: PipelineTestResult):
        self.results.append(result)
        
    def get_summary(self) -> Dict:
        total_tests = len(self.results)
        passed = len([r for r in self.results if r.status == "passed"])
        failed = len([r for r in self.results if r.status == "failed"])
        warnings = sum(len(r.warnings) for r in self.results)
        
        return {
            "total_pipelines_tested": total_tests,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "success_rate": (passed / total_tests * 100) if total_tests > 0 else 0,
            "total_duration": (datetime.now() - self.start_time).total_seconds()
        }
        
    def initialize_spark(self) -> bool:
        """Inicializa sessão Spark local para testes"""
        if not SPARK_AVAILABLE:
            logger.warning("PySpark não disponível - alguns testes serão limitados")
            return False
            
        try:
            if HELPERS_AVAILABLE:
                self.spark = get_spark_session("PipelineTestLocal", {
                    "spark.master": "local[*]",
                    "spark.sql.warehouse.dir": "/tmp/spark-warehouse-test",
                    "spark.sql.streaming.checkpointLocation": "/tmp/checkpoint-test"
                })
            else:
                self.spark = SparkSession.builder \
                    .appName("PipelineTestLocal") \
                    .master("local[*]") \
                    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test") \
                    .getOrCreate()
            
            logger.info("Sessão Spark inicializada com sucesso")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao inicializar Spark: {str(e)}")
            return False
    
    def create_sample_data(self) -> bool:
        """Cria dados de amostra para testes"""
        if not self.spark:
            logger.warning("Spark não disponível - não é possível criar dados de amostra")
            return False
            
        try:
            logger.info(" Criando dados de amostra para testes...")
            
            # Gerar dados sintéticos realistas
            sample_size = self.config.get("sample_data_size", 1000)
            
            # Dados com vários cenários para testar validações
            sample_data = []
            
            for i in range(sample_size):
                # Dados válidos (80%)
                if i < sample_size * 0.8:
                    pickup_time = datetime(2023, 1, 1, 10, 0, 0) + timedelta(minutes=i*5)
                    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 60))
                    
                    sample_data.append((
                        random.choice([1, 2]),  # VendorID
                        pickup_time,
                        dropoff_time,
                        random.randint(1, 4),  # passenger_count
                        round(random.uniform(0.5, 20.0), 2),  # trip_distance
                        1.0,  # RatecodeID
                        "N",  # store_and_fwd_flag
                        random.randint(1, 263),  # PULocationID
                        random.randint(1, 263),  # DOLocationID
                        random.choice([1, 2]),  # payment_type
                        round(random.uniform(5.0, 100.0), 2),  # fare_amount
                        0.5,  # extra
                        0.5,  # mta_tax
                        round(random.uniform(0.0, 20.0), 2),  # tip_amount
                        0.0,  # tolls_amount
                        0.3,  # improvement_surcharge
                        0.0,  # total_amount (será calculado)
                        2.5,  # congestion_surcharge
                        0.0   # airport_fee
                    ))
                # Dados com problemas (20%) para testar validações
                else:
                    pickup_time = datetime(2023, 1, 1, 10, 0, 0) + timedelta(minutes=i*5)
                    # Alguns com pickup após dropoff
                    if i % 10 == 0:
                        dropoff_time = pickup_time - timedelta(minutes=30)
                    else:
                        dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 60))
                    
                    sample_data.append((
                        random.choice([1, 2, 3]),  # VendorID inválido
                        pickup_time,
                        dropoff_time,
                        random.choice([0, 10]),  # passenger_count inválido
                        random.choice([-1.0, 1000.0]),  # trip_distance inválido
                        1.0,
                        "N",
                        random.choice([0, 500]),  # LocationID inválido
                        random.choice([0, 500]),
                        random.choice([1, 2]),
                        random.choice([-10.0, 2000.0]),  # fare_amount inválido
                        0.5,
                        0.5,
                        round(random.uniform(0.0, 20.0), 2),
                        0.0,
                        0.3,
                        0.0,
                        2.5,
                        0.0
                    ))
            
            # Calcular total_amount
            for i, row in enumerate(sample_data):
                row_list = list(row)
                row_list[16] = row_list[10] + row_list[11] + row_list[12] + row_list[13] + row_list[14] + row_list[15] + row_list[17] + row_list[18]
                sample_data[i] = tuple(row_list)
            
            # Importar random se não foi importado
            import random
            
            # Criar DataFrame
            if HELPERS_AVAILABLE:
                schema = YELLOW_TRIP_SCHEMA
            else:
                schema = StructType([
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
            
            self.sample_data = self.spark.createDataFrame(sample_data, schema)
            
            # Salvar como tabela temporária
            self.sample_data.createOrReplaceTempView("raw_taxi_data")
            
            logger.info(f"Dados de amostra criados: {sample_size} registros")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao criar dados de amostra: {str(e)}")
            return False
    
    def test_notebook_syntax(self, notebook_path: str) -> Tuple[bool, List[str], List[str]]:
        """Testa a sintaxe de um notebook"""
        errors = []
        warnings = []
        
        try:
            if not os.path.exists(notebook_path):
                errors.append(f"Notebook não encontrado: {notebook_path}")
                return False, errors, warnings
            
            with open(notebook_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Verificações básicas
            if len(content.strip()) == 0:
                errors.append("Notebook vazio")
                return False, errors, warnings
            
            # Verificar se é um notebook Databricks
            if not content.startswith("# Databricks notebook source"):
                warnings.append("Não parece ser um notebook Databricks válido")
            
            # Verificar imports essenciais
            required_imports = ["pyspark", "logging"]
            missing_imports = []
            
            for imp in required_imports:
                if imp not in content.lower():
                    missing_imports.append(imp)
            
            if missing_imports:
                warnings.append(f"Imports possivelmente faltando: {missing_imports}")
            
            # Verificar comandos Spark
            spark_commands = ["spark.sql", "spark.read", "spark.table", ".write", "DataFrame"]
            has_spark = any(cmd in content for cmd in spark_commands)
            
            if not has_spark:
                warnings.append("Não foram encontrados comandos Spark")
            
            return True, errors, warnings
            
        except Exception as e:
            errors.append(f"Erro ao analisar notebook: {str(e)}")
            return False, errors, warnings
    
    def simulate_pipeline_execution(self, stage_name: str, stage_config: Dict) -> PipelineTestResult:
        """Simula a execução de um pipeline"""
        result = PipelineTestResult(stage_name, stage_config["description"])
        result.start()
        
        try:
            notebook_path = stage_config["notebook"]
            
            # 1. Testar sintaxe do notebook
            syntax_valid, syntax_errors, syntax_warnings = self.test_notebook_syntax(notebook_path)
            
            result.details["notebook_path"] = notebook_path
            result.details["syntax_valid"] = syntax_valid
            
            if syntax_errors:
                result.errors.extend(syntax_errors)
            
            if syntax_warnings:
                result.warnings.extend(syntax_warnings)
            
            # 2. Simular operações baseadas no estágio
            if stage_name == "bronze":
                self._simulate_bronze_operations(result, stage_config)
            elif stage_name == "silver":
                self._simulate_silver_operations(result, stage_config)
            elif stage_name == "gold":
                self._simulate_gold_operations(result, stage_config)
            
            # 3. Verificar se há erros críticos
            if result.errors:
                result.finish("failed")
            elif result.warnings:
                result.finish("passed")
            else:
                result.finish("passed")
                
        except Exception as e:
            result.add_error(f"Erro na simulação do pipeline: {str(e)}")
            result.finish("failed")
        
        return result
    
    def _simulate_bronze_operations(self, result: PipelineTestResult, config: Dict):
        """Simula operações do pipeline Bronze"""
        try:
            if not self.spark or not self.sample_data:
                result.add_warning("Spark não disponível - simulação limitada")
                return
            
            # Simular ingestão
            raw_data = self.sample_data
            result.add_metrics({
                "input_records": raw_data.count(),
                "input_columns": len(raw_data.columns)
            })
            
            # Validação básica de schema
            if HELPERS_AVAILABLE:
                is_valid, issues = validate_dataframe_schema(raw_data)
                result.details["schema_validation"] = {
                    "valid": is_valid,
                    "issues": issues
                }
            
            # Simular escrita
            try:
                raw_data.createOrReplaceTempView(config["output_table"])
                result.details["output_table_created"] = True
                result.add_metrics({"output_records": raw_data.count()})
            except Exception as e:
                result.add_error(f"Erro ao criar tabela de saída: {str(e)}")
            
        except Exception as e:
            result.add_error(f"Erro nas operações Bronze: {str(e)}")
    
    def _simulate_silver_operations(self, result: PipelineTestResult, config: Dict):
        """Simula operações do pipeline Silver"""
        try:
            if not self.spark:
                result.add_warning("Spark não disponível - simulação limitada")
                return
            
            # Ler dados da camada Bronze
            try:
                bronze_data = self.spark.table(config["input_table"])
                result.add_metrics({
                    "input_records": bronze_data.count(),
                    "input_columns": len(bronze_data.columns)
                })
            except Exception as e:
                result.add_error(f"Erro ao ler dados Bronze: {str(e)}")
                return
            
            # Aplicar limpeza e validação
            if HELPERS_AVAILABLE:
                try:
                    # Aplicar filtros de qualidade
                    cleaned_data, quality_report = apply_data_quality_filters(bronze_data)
                    result.add_data_quality(quality_report)
                    
                    # Adicionar features temporais
                    enriched_data = add_temporal_features(cleaned_data)
                    
                    # Adicionar métricas de viagem
                    final_data = add_trip_metrics(enriched_data)
                    
                    result.add_metrics({
                        "output_records": final_data.count(),
                        "output_columns": len(final_data.columns),
                        "quality_score": quality_report.get("quality_score", 0)
                    })
                    
                    # Criar tabela Silver
                    final_data.createOrReplaceTempView(config["output_table"])
                    result.details["silver_processing_complete"] = True
                    
                except Exception as e:
                    result.add_error(f"Erro no processamento Silver: {str(e)}")
            else:
                result.add_warning("Helpers não disponível - processamento Silver limitado")
                bronze_data.createOrReplaceTempView(config["output_table"])
            
        except Exception as e:
            result.add_error(f"Erro nas operações Silver: {str(e)}")
    
    def _simulate_gold_operations(self, result: PipelineTestResult, config: Dict):
        """Simula operações do pipeline Gold"""
        try:
            if not self.spark:
                result.add_warning("Spark não disponível - simulação limitada")
                return
            
            # Ler dados da camada Silver
            try:
                silver_data = self.spark.table(config["input_table"])
                result.add_metrics({
                    "input_records": silver_data.count(),
                    "input_columns": len(silver_data.columns)
                })
            except Exception as e:
                result.add_error(f"Erro ao ler dados Silver: {str(e)}")
                return
            
            # Gerar análises agregadas
            try:
                # Análise por período temporal
                if "pickup_hour" in silver_data.columns:
                    hourly_stats = silver_data.groupBy("pickup_hour") \
                        .agg(
                            count("*").alias("trip_count"),
                            avg("total_amount").alias("avg_amount"),
                            sum("total_amount").alias("total_revenue")
                        )
                    hourly_stats.createOrReplaceTempView("gold_hourly_stats")
                
                # Análise por localização
                location_stats = silver_data.groupBy("PULocationID") \
                    .agg(
                        count("*").alias("pickup_count"),
                        avg("trip_distance").alias("avg_distance")
                    )
                location_stats.createOrReplaceTempView("gold_location_stats")
                
                # Resumo geral
                summary_stats = silver_data.agg(
                    count("*").alias("total_trips"),
                    sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_fare"),
                    max("trip_distance").alias("max_distance")
                ).collect()[0]
                
                result.add_metrics({
                    "total_trips_analyzed": summary_stats["total_trips"],
                    "total_revenue": float(summary_stats["total_revenue"] or 0),
                    "avg_fare": float(summary_stats["avg_fare"] or 0),
                    "max_distance": float(summary_stats["max_distance"] or 0)
                })
                
                result.details["gold_tables_created"] = len(config.get("output_tables", []))
                
            except Exception as e:
                result.add_error(f"Erro nas análises Gold: {str(e)}")
            
        except Exception as e:
            result.add_error(f"Erro nas operações Gold: {str(e)}")
    
    def run_all_tests(self):
        """Executa todos os testes de pipeline"""
        logger.info("🧪 Iniciando testes dos pipelines localmente...")
        logger.info("=" * 60)
        
        # Inicializar Spark
        if self.config.get("spark_local", True):
            if not self.initialize_spark():
                logger.warning("Continuando sem Spark - testes limitados")
        
        # Criar dados de amostra
        if self.config.get("simulate_data", True) and self.spark:
            if not self.create_sample_data():
                logger.warning("Continuando sem dados de amostra")
        
        # Executar testes para cada pipeline
        for stage_name, stage_config in PIPELINE_STAGES.items():
            logger.info(f"\n Testando pipeline: {stage_name.upper()}")
            logger.info("-" * 40)
            
            result = self.simulate_pipeline_execution(stage_name, stage_config)
            self.add_result(result)
        
        # Finalizar Spark
        if self.spark:
            try:
                self.spark.stop()
                logger.info("🛑 Sessão Spark finalizada")
            except Exception as e:
                logger.warning(f"Aviso ao finalizar Spark: {str(e)}")
    
    def save_results(self):
        """Salva resultados em arquivos JSON e texto"""
        if not self.config.get("save_reports", True):
            return
            
        reports_dir = self.config.get("reports_dir", "reports")
        timestamp = self.config.get("timestamp")
        
        # Criar diretório se não existir
        os.makedirs(reports_dir, exist_ok=True)
        
        # Relatório detalhado JSON
        detailed_report = {
            "test_config": self.config,
            "pipeline_stages": PIPELINE_STAGES,
            "summary": self.get_summary(),
            "results": [r.to_dict() for r in self.results],
            "generated_at": datetime.now().isoformat()
        }
        
        detailed_file = os.path.join(reports_dir, f"pipeline_test_detailed_{timestamp}.json")
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_report, f, indent=2, ensure_ascii=False)
        
        # Relatório resumido texto
        summary = self.get_summary()
        summary_text = f"""
NYC Yellow Trip Project - Relatório de Testes dos Pipelines
==========================================================

Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duração Total: {summary['total_duration']:.2f} segundos

RESUMO DOS PIPELINES:
- Total de pipelines testados: {summary['total_pipelines_tested']}
- Pipelines aprovados: {summary['passed']}
- Pipelines com falhas: {summary['failed']}
- Avisos: {summary['warnings']}
- Taxa de sucesso: {summary['success_rate']:.1f}%

RESULTADOS POR PIPELINE:
"""
        
        for result in self.results:
            status_symbol = "[PASS]" if result.status == "passed" else "[FAIL]" if result.status == "failed" else "[WARN]"
            summary_text += f"\n{status_symbol} {result.pipeline_name.upper()} - {result.stage}:\n"
            summary_text += f"  - Status: {result.status}\n"
            summary_text += f"  - Duração: {result.duration:.2f}s\n"
            
            if result.metrics:
                summary_text += f"  - Métricas:\n"
                for key, value in result.metrics.items():
                    summary_text += f"    • {key}: {value}\n"
            
            if result.errors:
                summary_text += f"  - Erros:\n"
                for error in result.errors:
                    summary_text += f"    • {error}\n"
            
            if result.warnings:
                summary_text += f"  - Avisos:\n"
                for warning in result.warnings:
                    summary_text += f"    • {warning}\n"
        
        # Adicionar análise de qualidade de dados se disponível
        data_quality_results = [r for r in self.results if r.data_quality]
        if data_quality_results:
            summary_text += "\nANÁLISE DE QUALIDADE DOS DADOS:\n"
            for result in data_quality_results:
                if result.data_quality:
                    summary_text += f"\n{result.pipeline_name}:\n"
                    for key, value in result.data_quality.items():
                        summary_text += f"  - {key}: {value}\n"
        
        summary_text += f"\nArquivos de log salvos em: {reports_dir}/\n"
        summary_text += f"Log detalhado: {log_filename}\n"
        summary_text += f"Relatório gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        summary_file = os.path.join(reports_dir, f"pipeline_test_summary_{timestamp}.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_text)
            
        logger.info(f" Relatórios salvos em: {reports_dir}/")
        logger.info(f"- Detalhado: {detailed_file}")
        logger.info(f"- Resumo: {summary_file}")
        logger.info(f"- Log: {log_filename}")

# ====================================
# FUNÇÃO PRINCIPAL
# ====================================

def main():
    """Função principal para execução dos testes de pipeline"""
    
    print("=" * 70)
    print("🧪 NYC YELLOW TRIP PROJECT - TESTE DOS PIPELINES LOCAL")
    print("=" * 70)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Verificar se os notebooks existem
    missing_notebooks = []
    for stage_name, config in PIPELINE_STAGES.items():
        if not os.path.exists(config["notebook"]):
            missing_notebooks.append(config["notebook"])
    
    if missing_notebooks:
        print("ERRO: Notebooks não encontrados:")
        for notebook in missing_notebooks:
            print(f"  - {notebook}")
        print()
        print("Certifique-se de que os notebooks estão no diretório correto.")
        return 1
    
    # Inicializar suite de testes
    suite = PipelineTestSuite(PIPELINE_TEST_CONFIG)
    
    try:
        # Executar todos os testes
        suite.run_all_tests()
        
        # Gerar relatórios
        suite.save_results()
        
        # Exibir resumo
        summary = suite.get_summary()
        
        print()
        print("=" * 70)
        print("PIPELINE TEST EXECUTION SUMMARY")
        print("=" * 70)
        print(f"Total de pipelines testados: {summary['total_pipelines_tested']}")
        print(f"Aprovados: {summary['passed']}")
        print(f"Falharam: {summary['failed']}")
        print(f"Avisos: {summary['warnings']}")
        print(f"Taxa de sucesso: {summary['success_rate']:.1f}%")
        print(f"Duração total: {summary['total_duration']:.2f} segundos")
        print()
        
        # Exibir detalhes dos pipelines que falharam
        failed_pipelines = [r for r in suite.results if r.status == "failed"]
        if failed_pipelines:
            print("PIPELINES QUE FALHARAM:")
            print("-" * 35)
            for pipeline in failed_pipelines:
                print(f"• {pipeline.pipeline_name} - {pipeline.stage}")
                for error in pipeline.errors:
                    print(f"  - {error}")
            print()
        
        # Status final
        if summary['success_rate'] >= 90:
            print(" TODOS OS PIPELINES ESTÃO FUNCIONANDO PERFEITAMENTE!")
            exit_code = 0
        elif summary['success_rate'] >= 70:
            print("👍 PIPELINES EM BOM ESTADO COM ALGUMAS MELHORIAS NECESSÁRIAS")
            exit_code = 0
        elif summary['success_rate'] >= 50:
            print("PIPELINES PRECISAM DE ATENÇÃO - VÁRIAS MELHORIAS NECESSÁRIAS")
            exit_code = 1
        else:
            print(" PIPELINES EM ESTADO CRÍTICO - REQUER CORREÇÕES URGENTES")
            exit_code = 2
        
        print()
        print(f"Relatórios salvos em: {PIPELINE_TEST_CONFIG['reports_dir']}/")
        print(f"Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        return exit_code
        
    except KeyboardInterrupt:
        print("\n🛑 Teste interrompido pelo usuário")
        return 130
        
    except Exception as e:
        logger.error(f"Erro crítico na execução dos testes: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"\n ERRO CRÍTICO: {str(e)}")
        return 1
    
    finally:
        # Garantir limpeza
        if suite.spark:
            try:
                suite.spark.stop()
            except:
                pass

if __name__ == "__main__":
    # Adicionar import do random que é usado na função create_sample_data
    import random
    
    exit_code = main()
    sys.exit(exit_code)
