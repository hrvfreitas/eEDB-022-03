"""
Configuração de conexão com o PostgreSQL, usada por todas as etapas do
pipeline Spark (RAW / Trusted / Delivery).

As credenciais são lidas de variáveis de ambiente, com valores default
compatíveis com o docker-compose deste repositório.
"""
import os

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
DB_NAME = os.environ.get("DB_NAME", "eedb")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_table(df, schema: str, table: str, mode: str = "overwrite"):
    """Escreve um DataFrame Spark em uma tabela do Postgres (schema.table)."""
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"{schema}.{table}")
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .mode(mode)
        .save()
    )


def read_table(spark, schema: str, table: str):
    """Lê uma tabela do Postgres (schema.table) como DataFrame Spark."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"{schema}.{table}")
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .load()
    )
