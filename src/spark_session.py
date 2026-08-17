"""
Cria a SparkSession usada em todas as etapas do pipeline.

O driver JDBC do PostgreSQL (postgresql-42.x.jar) é baixado automaticamente
via pacote Maven (org.postgresql:postgresql), tanto em execução local
quanto em cluster, através da opção spark.jars.packages.
"""
from pyspark.sql import SparkSession

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .master(__import__("os").environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
