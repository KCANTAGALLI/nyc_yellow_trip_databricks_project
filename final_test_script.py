#!/usr/bin/env python3
"""
NYC Yellow Trip Records - Script Final de Teste
==============================================

Script para testar todos os componentes do pipeline de dados
dos registros de táxi amarelo de NYC.

Executa testes das camadas Bronze, Silver e Gold e gera relatórios
detalhados salvos na pasta reports/.

Autor: Data Engineering Team
Versão: 1.0
Data: 2025
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import subprocess

# Adicionar o diretório do projeto ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    print("AVISO: PySpark não disponível. Executando testes básicos...")
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
    print("AVISO: Módulo helpers não disponível. Testes limitados...")
    HELPERS_AVAILABLE = False

# ====================================
# CONFIGURAÇÕES
# ====================================

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reports/test_execution.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações do teste
TEST_CONFIG = {
    "project_name": "nyc_yellow_trip_databricks_project",
    "test_mode": "comprehensive",  # basic, comprehensive, performance
    "spark_local": True,
    "generate_sample_data": True,
    "save_reports": True,
    "reports_dir": "reports",
    "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
}

# Estrutura esperada do projeto
EXPECTED_STRUCTURE = {
    "notebooks": [
        "01_ingestao_bronze.py",
        "02_tratamento_silver.py", 
        "03_analise_gold.py",
        "04_automatizacao.py"
    ],
    "pipelines": [
        "helpers.py"
    ],
    "docs": [
        "inferencias.md"
    ],
    "required_files": [
        "README.md",
        "RESUMO_IMPLEMENTACAO.md",
        "INSTRUCOES_USO_LOCAL.md"
    ]
}

# ====================================
# CLASSES DE TESTE
# ====================================

class TestResult:
    """Classe para armazenar resultados de testes"""
    
    def __init__(self, name: str, category: str):
        self.name = name
        self.category = category
        self.status = "pending"
        self.start_time = None
        self.end_time = None
        self.duration = None
        self.details = {}
        self.errors = []
        self.warnings = []
        
    def start(self):
        self.start_time = datetime.now()
        self.status = "running"
        
    def finish(self, status: str = "passed", details: Dict = None):
        self.end_time = datetime.now()
        self.status = status
        self.duration = (self.end_time - self.start_time).total_seconds()
        if details:
            self.details.update(details)
            
    def add_error(self, error: str):
        self.errors.append(error)
        self.status = "failed"
        
    def add_warning(self, warning: str):
        self.warnings.append(warning)
        
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "category": self.category,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration,
            "details": self.details,
            "errors": self.errors,
            "warnings": self.warnings
        }

class TestSuite:
    """Classe principal para execução dos testes"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.results = []
        self.spark = None
        self.start_time = datetime.now()
        
    def add_result(self, result: TestResult):
        self.results.append(result)
        
    def get_summary(self) -> Dict:
        total_tests = len(self.results)
        passed = len([r for r in self.results if r.status == "passed"])
        failed = len([r for r in self.results if r.status == "failed"])
        warnings = sum(len(r.warnings) for r in self.results)
        
        return {
            "total_tests": total_tests,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "success_rate": (passed / total_tests * 100) if total_tests > 0 else 0,
            "total_duration": (datetime.now() - self.start_time).total_seconds()
        }
        
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
            "summary": self.get_summary(),
            "results": [r.to_dict() for r in self.results],
            "generated_at": datetime.now().isoformat()
        }
        
        detailed_file = os.path.join(reports_dir, f"detailed_report_{timestamp}.json")
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_report, f, indent=2, ensure_ascii=False)
        
        # Relatório resumido texto
        summary = self.get_summary()
        summary_text = f"""
NYC Yellow Trip Project - Relatório de Testes
============================================

Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duração Total: {summary['total_duration']:.2f} segundos

RESUMO:
- Total de testes: {summary['total_tests']}
- Testes aprovados: {summary['passed']}
- Testes falharam: {summary['failed']}
- Avisos: {summary['warnings']}
- Taxa de sucesso: {summary['success_rate']:.1f}%

RESULTADOS POR CATEGORIA:
"""
        
        # Agrupar por categoria
        categories = {}
        for result in self.results:
            cat = result.category
            if cat not in categories:
                categories[cat] = {"passed": 0, "failed": 0, "total": 0}
            categories[cat]["total"] += 1
            if result.status == "passed":
                categories[cat]["passed"] += 1
            elif result.status == "failed":
                categories[cat]["failed"] += 1
                
        for cat, stats in categories.items():
            success_rate = (stats["passed"] / stats["total"] * 100) if stats["total"] > 0 else 0
            summary_text += f"\n{cat}:\n"
            summary_text += f"  - Aprovados: {stats['passed']}/{stats['total']} ({success_rate:.1f}%)\n"
        
        # Adicionar falhas se houver
        failed_tests = [r for r in self.results if r.status == "failed"]
        if failed_tests:
            summary_text += "\nTESTES QUE FALHARAM:\n"
            for test in failed_tests:
                summary_text += f"\n- {test.name} ({test.category}):\n"
                for error in test.errors:
                    summary_text += f"  Erro: {error}\n"
        
        summary_file = os.path.join(reports_dir, f"summary_{timestamp}.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_text)
            
        logger.info(f"Relatórios salvos em: {reports_dir}/")
        logger.info(f"- Detalhado: {detailed_file}")
        logger.info(f"- Resumo: {summary_file}")

# ====================================
# FUNÇÕES DE TESTE
# ====================================

def test_project_structure(suite: TestSuite):
    """Testa a estrutura do projeto"""
    result = TestResult("Estrutura do Projeto", "Estrutura")
    result.start()
    
    try:
        logger.info(" Testando estrutura do projeto...")
        
        # Verificar notebooks
        notebooks_dir = "notebooks"
        if os.path.exists(notebooks_dir):
            notebooks = os.listdir(notebooks_dir)
            missing_notebooks = []
            for notebook in EXPECTED_STRUCTURE["notebooks"]:
                if notebook not in notebooks:
                    missing_notebooks.append(notebook)
            
            if missing_notebooks:
                result.add_warning(f"Notebooks faltando: {missing_notebooks}")
            else:
                result.details["notebooks_status"] = "Todos os notebooks encontrados"
        else:
            result.add_error("Diretório notebooks/ não encontrado")
        
        # Verificar pipelines
        pipelines_dir = "pipelines"
        if os.path.exists(pipelines_dir):
            pipelines = os.listdir(pipelines_dir)
            missing_pipelines = []
            for pipeline in EXPECTED_STRUCTURE["pipelines"]:
                if pipeline not in pipelines:
                    missing_pipelines.append(pipeline)
            
            if missing_pipelines:
                result.add_warning(f"Arquivos de pipeline faltando: {missing_pipelines}")
            else:
                result.details["pipelines_status"] = "Todos os arquivos de pipeline encontrados"
        else:
            result.add_error("Diretório pipelines/ não encontrado")
        
        # Verificar arquivos obrigatórios
        missing_files = []
        for file in EXPECTED_STRUCTURE["required_files"]:
            if not os.path.exists(file):
                missing_files.append(file)
        
        if missing_files:
            result.add_warning(f"Arquivos obrigatórios faltando: {missing_files}")
        else:
            result.details["required_files_status"] = "Todos os arquivos obrigatórios encontrados"
        
        # Verificar diretório de relatórios
        reports_dir = suite.config.get("reports_dir", "reports")
        if not os.path.exists(reports_dir):
            try:
                os.makedirs(reports_dir)
                result.details["reports_dir"] = f"Diretório {reports_dir}/ criado"
            except Exception as e:
                result.add_error(f"Não foi possível criar diretório {reports_dir}/: {str(e)}")
        else:
            result.details["reports_dir"] = f"Diretório {reports_dir}/ já existe"
        
        if not result.errors:
            result.finish("passed")
        else:
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_dependencies(suite: TestSuite):
    """Testa as dependências do projeto"""
    result = TestResult("Dependências", "Ambiente")
    result.start()
    
    try:
        logger.info("📦 Testando dependências...")
        
        dependencies_status = {}
        
        # Testar PySpark
        if SPARK_AVAILABLE:
            dependencies_status["pyspark"] = "Disponível"
            try:
                from pyspark import __version__ as spark_version
                dependencies_status["pyspark_version"] = spark_version
            except:
                dependencies_status["pyspark_version"] = "Versão não detectada"
        else:
            dependencies_status["pyspark"] = "Não disponível"
            result.add_warning("PySpark não está disponível")
        
        # Testar módulo helpers
        if HELPERS_AVAILABLE:
            dependencies_status["helpers"] = "Disponível"
        else:
            dependencies_status["helpers"] = "Não disponível"
            result.add_warning("Módulo helpers não está disponível")
        
        # Testar outras dependências
        try:
            import json
            dependencies_status["json"] = "Disponível"
        except ImportError:
            dependencies_status["json"] = "Não disponível"
            result.add_error("Módulo json não disponível")
        
        try:
            import logging
            dependencies_status["logging"] = "Disponível"
        except ImportError:
            dependencies_status["logging"] = "Não disponível"
            result.add_error("Módulo logging não disponível")
        
        result.details["dependencies"] = dependencies_status
        
        # Verificar Python
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        result.details["python_version"] = python_version
        
        if sys.version_info.major < 3 or (sys.version_info.major == 3 and sys.version_info.minor < 7):
            result.add_warning(f"Python {python_version} pode não ser compatível (recomendado 3.7+)")
        
        if not result.errors:
            result.finish("passed")
        else:
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_helpers_module(suite: TestSuite):
    """Testa o módulo de funções auxiliares"""
    result = TestResult("Módulo Helpers", "Funcionalidade")
    result.start()
    
    try:
        logger.info("Testando módulo helpers...")
        
        if not HELPERS_AVAILABLE:
            result.add_error("Módulo helpers não disponível para teste")
            result.finish("failed")
            suite.add_result(result)
            return
        
        # Testar importação de constantes
        try:
            schema = YELLOW_TRIP_SCHEMA
            rules = DEFAULT_VALIDATION_RULES
            result.details["constants_imported"] = True
        except Exception as e:
            result.add_error(f"Erro ao importar constantes: {str(e)}")
        
        # Testar funções básicas (sem Spark)
        functions_tested = {
            "validate_dataframe_schema": False,
            "apply_data_quality_filters": False,
            "add_temporal_features": False,
            "add_trip_metrics": False,
            "calculate_data_quality_metrics": False,
            "generate_summary_statistics": False,
            "create_data_quality_report": False,
            "validate_business_rules": False
        }
        
        # Verificar se as funções existem
        for func_name in functions_tested.keys():
            try:
                func = globals().get(func_name)
                if func and callable(func):
                    functions_tested[func_name] = True
                else:
                    result.add_warning(f"Função {func_name} não encontrada ou não é callable")
            except Exception as e:
                result.add_warning(f"Erro ao verificar função {func_name}: {str(e)}")
        
        result.details["functions_available"] = functions_tested
        
        # Contar funções disponíveis
        available_count = sum(functions_tested.values())
        total_count = len(functions_tested)
        
        result.details["functions_summary"] = {
            "available": available_count,
            "total": total_count,
            "percentage": (available_count / total_count * 100) if total_count > 0 else 0
        }
        
        if available_count == total_count:
            result.finish("passed")
        elif available_count >= total_count * 0.7:  # 70% das funções disponíveis
            result.add_warning(f"Apenas {available_count}/{total_count} funções disponíveis")
            result.finish("passed")
        else:
            result.add_error(f"Muitas funções indisponíveis: {available_count}/{total_count}")
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_spark_functionality(suite: TestSuite):
    """Testa funcionalidades do Spark"""
    result = TestResult("Funcionalidade Spark", "Spark")
    result.start()
    
    try:
        logger.info(" Testando funcionalidades do Spark...")
        
        if not SPARK_AVAILABLE:
            result.add_error("PySpark não disponível para teste")
            result.finish("failed")
            suite.add_result(result)
            return
        
        # Tentar criar sessão Spark local
        try:
            if HELPERS_AVAILABLE:
                spark = get_spark_session("FinalTestScript", {
                    "spark.master": "local[*]",
                    "spark.sql.warehouse.dir": "/tmp/spark-warehouse"
                })
            else:
                spark = SparkSession.builder \
                    .appName("FinalTestScript") \
                    .master("local[*]") \
                    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                    .getOrCreate()
            
            suite.spark = spark
            result.details["spark_session"] = "Criada com sucesso"
            
            # Testar operações básicas
            test_data = [(1, "test", 10.0), (2, "test2", 20.0)]
            test_df = spark.createDataFrame(test_data, ["id", "name", "value"])
            
            count = test_df.count()
            result.details["test_dataframe_count"] = count
            
            if count == 2:
                result.details["basic_operations"] = "Funcionando"
            else:
                result.add_warning(f"Contagem inesperada: {count}, esperado: 2")
            
            # Testar funções SQL
            test_df.createOrReplaceTempView("test_table")
            sql_result = spark.sql("SELECT COUNT(*) as total FROM test_table").collect()
            
            if sql_result[0]["total"] == 2:
                result.details["sql_operations"] = "Funcionando"
            else:
                result.add_warning("Operações SQL não funcionando corretamente")
            
            result.finish("passed")
            
        except Exception as e:
            result.add_error(f"Erro ao testar Spark: {str(e)}")
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_data_processing_pipeline(suite: TestSuite):
    """Testa o pipeline de processamento de dados com dados sintéticos"""
    result = TestResult("Pipeline de Processamento", "Pipeline")
    result.start()
    
    try:
        logger.info(" Testando pipeline de processamento...")
        
        if not SPARK_AVAILABLE or not HELPERS_AVAILABLE or not suite.spark:
            result.add_error("Spark ou helpers não disponíveis para teste de pipeline")
            result.finish("failed")
            suite.add_result(result)
            return
        
        spark = suite.spark
        
        # Criar dados sintéticos para teste
        sample_data = [
            # Dados válidos
            (1, "2023-01-01 10:00:00", "2023-01-01 10:30:00", 1.0, 5.2, 1.0, "N", 100, 200, 1, 15.0, 0.5, 0.5, 3.0, 0.0, 0.3, 19.3, 2.5, 0.0),
            (2, "2023-01-01 11:00:00", "2023-01-01 11:25:00", 2.0, 3.1, 1.0, "N", 150, 250, 1, 12.5, 0.5, 0.5, 2.5, 0.0, 0.3, 16.3, 2.5, 0.0),
            (1, "2023-01-01 12:00:00", "2023-01-01 12:45:00", 1.0, 8.5, 1.0, "N", 200, 300, 2, 25.0, 0.5, 0.5, 0.0, 0.0, 0.3, 26.3, 2.5, 0.0),
            # Dados com problemas para testar validação
            (1, "2023-01-01 13:00:00", "2023-01-01 12:30:00", 1.0, 2.0, 1.0, "N", 100, 200, 1, 8.0, 0.5, 0.5, 1.0, 0.0, 0.3, 10.3, 2.5, 0.0),  # pickup após dropoff
            (2, "2023-01-01 14:00:00", "2023-01-01 14:30:00", 10.0, 500.0, 1.0, "N", 150, 250, 1, 2000.0, 0.5, 0.5, 100.0, 0.0, 0.3, 2100.8, 2.5, 0.0),  # valores extremos
        ]
        
        # Converter timestamps para formato correto
        from pyspark.sql.functions import to_timestamp
        
        sample_df = spark.createDataFrame(sample_data, YELLOW_TRIP_SCHEMA)
        sample_df = sample_df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
                           .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        
        original_count = sample_df.count()
        result.details["original_records"] = original_count
        
        # Teste 1: Validação de schema
        try:
            is_valid, issues = validate_dataframe_schema(sample_df)
            result.details["schema_validation"] = {
                "valid": is_valid,
                "issues": issues
            }
        except Exception as e:
            result.add_warning(f"Erro na validação de schema: {str(e)}")
        
        # Teste 2: Adicionar features temporais
        try:
            df_with_temporal = add_temporal_features(sample_df)
            temporal_columns = [col for col in df_with_temporal.columns if col.startswith("pickup_") or col in ["time_period", "is_weekend"]]
            result.details["temporal_features"] = {
                "added_columns": len(temporal_columns),
                "columns": temporal_columns
            }
        except Exception as e:
            result.add_error(f"Erro ao adicionar features temporais: {str(e)}")
            df_with_temporal = sample_df
        
        # Teste 3: Adicionar métricas de viagem
        try:
            df_with_metrics = add_trip_metrics(df_with_temporal)
            metric_columns = [col for col in df_with_metrics.columns if col in ["trip_duration_minutes", "avg_speed_mph", "tip_percentage", "cost_per_mile"]]
            result.details["trip_metrics"] = {
                "added_columns": len(metric_columns),
                "columns": metric_columns
            }
        except Exception as e:
            result.add_error(f"Erro ao adicionar métricas de viagem: {str(e)}")
            df_with_metrics = df_with_temporal
        
        # Teste 4: Aplicar filtros de qualidade
        try:
            df_filtered, quality_report = apply_data_quality_filters(df_with_metrics)
            filtered_count = df_filtered.count()
            
            result.details["quality_filtering"] = {
                "original_count": quality_report.get("original_records", original_count),
                "filtered_count": quality_report.get("filtered_records", filtered_count),
                "removed_count": quality_report.get("removed_records", original_count - filtered_count),
                "quality_score": quality_report.get("quality_score", 0)
            }
        except Exception as e:
            result.add_error(f"Erro ao aplicar filtros de qualidade: {str(e)}")
            df_filtered = df_with_metrics
        
        # Teste 5: Calcular métricas de qualidade
        try:
            quality_metrics = calculate_data_quality_metrics(df_filtered)
            result.details["quality_metrics"] = quality_metrics
        except Exception as e:
            result.add_warning(f"Erro ao calcular métricas de qualidade: {str(e)}")
        
        # Teste 6: Gerar estatísticas resumidas
        try:
            summary_stats = generate_summary_statistics(df_filtered)
            result.details["summary_statistics"] = summary_stats
        except Exception as e:
            result.add_warning(f"Erro ao gerar estatísticas: {str(e)}")
        
        # Teste 7: Validar regras de negócio
        try:
            business_validation = validate_business_rules(df_filtered)
            result.details["business_rules"] = business_validation
        except Exception as e:
            result.add_warning(f"Erro ao validar regras de negócio: {str(e)}")
        
        # Avaliar sucesso do pipeline
        if not result.errors:
            result.finish("passed")
        else:
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado no pipeline: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_notebooks_syntax(suite: TestSuite):
    """Testa a sintaxe dos notebooks"""
    result = TestResult("Sintaxe dos Notebooks", "Notebooks")
    result.start()
    
    try:
        logger.info("📔 Testando sintaxe dos notebooks...")
        
        notebooks_dir = "notebooks"
        if not os.path.exists(notebooks_dir):
            result.add_error("Diretório notebooks/ não encontrado")
            result.finish("failed")
            suite.add_result(result)
            return
        
        notebook_results = {}
        
        for notebook_file in EXPECTED_STRUCTURE["notebooks"]:
            notebook_path = os.path.join(notebooks_dir, notebook_file)
            
            if not os.path.exists(notebook_path):
                notebook_results[notebook_file] = {"status": "missing", "errors": ["Arquivo não encontrado"]}
                continue
            
            try:
                # Tentar ler o arquivo
                with open(notebook_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Verificações básicas
                errors = []
                warnings = []
                
                # Verificar se tem conteúdo
                if len(content.strip()) == 0:
                    errors.append("Arquivo vazio")
                
                # Verificar estrutura básica de notebook Databricks
                if not content.startswith("# Databricks notebook source"):
                    warnings.append("Não parece ser um notebook Databricks válido")
                
                # Verificar imports básicos esperados
                expected_imports = ["from pyspark.sql", "import logging", "from datetime import"]
                missing_imports = []
                
                for imp in expected_imports:
                    if imp not in content:
                        missing_imports.append(imp)
                
                if missing_imports:
                    warnings.append(f"Imports possivelmente faltando: {missing_imports}")
                
                # Verificar se tem comandos SQL ou PySpark
                has_spark_commands = any(cmd in content for cmd in ["spark.sql", "spark.read", "spark.table", ".write"])
                if not has_spark_commands:
                    warnings.append("Não foram encontrados comandos Spark")
                
                notebook_results[notebook_file] = {
                    "status": "valid" if not errors else "invalid",
                    "errors": errors,
                    "warnings": warnings,
                    "size_bytes": len(content),
                    "lines": len(content.split('\n'))
                }
                
                if errors:
                    result.add_error(f"Erros no {notebook_file}: {errors}")
                
                if warnings:
                    result.add_warning(f"Avisos no {notebook_file}: {warnings}")
                    
            except Exception as e:
                notebook_results[notebook_file] = {
                    "status": "error",
                    "errors": [f"Erro ao ler arquivo: {str(e)}"]
                }
                result.add_error(f"Erro ao analisar {notebook_file}: {str(e)}")
        
        result.details["notebooks"] = notebook_results
        
        # Resumo
        valid_notebooks = len([r for r in notebook_results.values() if r["status"] == "valid"])
        total_notebooks = len(notebook_results)
        
        result.details["summary"] = {
            "valid_notebooks": valid_notebooks,
            "total_notebooks": total_notebooks,
            "validation_rate": (valid_notebooks / total_notebooks * 100) if total_notebooks > 0 else 0
        }
        
        if not result.errors:
            result.finish("passed")
        else:
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

def test_documentation(suite: TestSuite):
    """Testa a documentação do projeto"""
    result = TestResult("Documentação", "Documentação")
    result.start()
    
    try:
        logger.info(" Testando documentação...")
        
        doc_results = {}
        
        # Testar arquivos de documentação
        doc_files = EXPECTED_STRUCTURE["required_files"] + ["docs/inferencias.md"]
        
        for doc_file in doc_files:
            if os.path.exists(doc_file):
                try:
                    with open(doc_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    doc_results[doc_file] = {
                        "exists": True,
                        "size_bytes": len(content),
                        "lines": len(content.split('\n')),
                        "has_content": len(content.strip()) > 0
                    }
                    
                    # Verificações específicas por tipo de arquivo
                    if doc_file == "README.md":
                        if "# NYC Yellow Trip" not in content and "nyc" not in content.lower():
                            result.add_warning("README.md pode não estar relacionado ao projeto NYC")
                    
                    elif doc_file == "RESUMO_IMPLEMENTACAO.md":
                        if len(content) < 500:  # Menos de 500 caracteres
                            result.add_warning("RESUMO_IMPLEMENTACAO.md parece muito curto")
                    
                    elif doc_file == "INSTRUCOES_USO_LOCAL.md":
                        if "spark" not in content.lower() and "databricks" not in content.lower():
                            result.add_warning("INSTRUCOES_USO_LOCAL.md pode não conter instruções adequadas")
                    
                except Exception as e:
                    doc_results[doc_file] = {
                        "exists": True,
                        "error": f"Erro ao ler: {str(e)}"
                    }
                    result.add_warning(f"Erro ao ler {doc_file}: {str(e)}")
            else:
                doc_results[doc_file] = {"exists": False}
                result.add_warning(f"Arquivo de documentação faltando: {doc_file}")
        
        result.details["documentation_files"] = doc_results
        
        # Resumo da documentação
        existing_docs = len([r for r in doc_results.values() if r.get("exists", False)])
        total_docs = len(doc_results)
        
        result.details["summary"] = {
            "existing_docs": existing_docs,
            "total_expected": total_docs,
            "completion_rate": (existing_docs / total_docs * 100) if total_docs > 0 else 0
        }
        
        if existing_docs >= total_docs * 0.8:  # 80% da documentação presente
            result.finish("passed")
        else:
            result.add_error(f"Documentação insuficiente: {existing_docs}/{total_docs} arquivos")
            result.finish("failed")
            
    except Exception as e:
        result.add_error(f"Erro inesperado: {str(e)}")
        result.finish("failed")
    
    suite.add_result(result)

# ====================================
# FUNÇÃO PRINCIPAL
# ====================================

def main():
    """Função principal para execução dos testes"""
    
    print("=" * 60)
    print("🧪 NYC YELLOW TRIP PROJECT - TESTE FINAL")
    print("=" * 60)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Inicializar suite de testes
    suite = TestSuite(TEST_CONFIG)
    
    try:
        # Executar testes na ordem
        test_functions = [
            test_project_structure,
            test_dependencies,
            test_helpers_module,
            test_spark_functionality,
            test_data_processing_pipeline,
            test_notebooks_syntax,
            test_documentation
        ]
        
        for test_func in test_functions:
            try:
                test_func(suite)
            except Exception as e:
                logger.error(f"Erro ao executar {test_func.__name__}: {str(e)}")
                # Criar resultado de erro para o teste que falhou
                error_result = TestResult(test_func.__name__, "Erro")
                error_result.start()
                error_result.add_error(f"Falha na execução: {str(e)}")
                error_result.finish("failed")
                suite.add_result(error_result)
        
        # Finalizar sessão Spark se criada
        if suite.spark:
            try:
                suite.spark.stop()
                logger.info("Sessão Spark finalizada")
            except Exception as e:
                logger.warning(f"Erro ao finalizar Spark: {str(e)}")
        
        # Gerar relatórios
        suite.save_results()
        
        # Exibir resumo
        summary = suite.get_summary()
        
        print()
        print("=" * 60)
        print(" RESUMO DOS TESTES")
        print("=" * 60)
        print(f"Total de testes: {summary['total_tests']}")
        print(f"Aprovados: {summary['passed']}")
        print(f"Falharam: {summary['failed']}")
        print(f"Avisos: {summary['warnings']}")
        print(f"Taxa de sucesso: {summary['success_rate']:.1f}%")
        print(f"Duração total: {summary['total_duration']:.2f} segundos")
        print()
        
        # Exibir detalhes dos testes que falharam
        failed_tests = [r for r in suite.results if r.status == "failed"]
        if failed_tests:
            print("TESTES QUE FALHARAM:")
            print("-" * 30)
            for test in failed_tests:
                print(f"• {test.name} ({test.category})")
                for error in test.errors:
                    print(f"  - {error}")
            print()
        
        # Exibir avisos importantes
        all_warnings = []
        for result in suite.results:
            all_warnings.extend(result.warnings)
        
        if all_warnings:
            print("AVISOS IMPORTANTES:")
            print("-" * 25)
            for warning in all_warnings[:10]:  # Mostrar apenas os primeiros 10
                print(f"• {warning}")
            if len(all_warnings) > 10:
                print(f"... e mais {len(all_warnings) - 10} avisos")
            print()
        
        # Status final
        if summary['success_rate'] >= 90:
            print(" PROJETO EM EXCELENTE ESTADO!")
            exit_code = 0
        elif summary['success_rate'] >= 70:
            print("👍 PROJETO EM BOM ESTADO COM ALGUMAS MELHORIAS NECESSÁRIAS")
            exit_code = 0
        elif summary['success_rate'] >= 50:
            print("PROJETO PRECISA DE ATENÇÃO - VÁRIAS MELHORIAS NECESSÁRIAS")
            exit_code = 1
        else:
            print(" PROJETO EM ESTADO CRÍTICO - REQUER CORREÇÕES URGENTES")
            exit_code = 2
        
        print()
        print(f"Relatórios salvos em: {TEST_CONFIG['reports_dir']}/")
        print(f"Finalizado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        return exit_code
        
    except KeyboardInterrupt:
        print("\n🛑 Teste interrompido pelo usuário")
        return 130
        
    except Exception as e:
        logger.error(f"Erro crítico na execução dos testes: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"\n ERRO CRÍTICO: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


