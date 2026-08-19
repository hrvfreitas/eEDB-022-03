""" Cria a SparkSession usada em todas as etapas do pipeline.
O driver JDBC do PostgreSQL é baixado automaticamente via pacote Maven.
"""
from pyspark.sql import SparkSession

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)  # baixa driver JDBC
             .config("spark.sql.session.timeZone", "UTC")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))   # default: local mode
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
