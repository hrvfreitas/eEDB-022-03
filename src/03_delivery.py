"""
Etapa 3 — Camada Delivery

Lê as 3 tabelas Trusted (Postgres) e realiza, via Spark DataFrame API
(joins/agregações — nenhuma etapa em SQL), a união final por CNPJ,
gerando a tabela:

    delivery.tb_reclamacoes_bancos_funcionarios

Granularidade: banco x trimestre. Grava em Parquet (data/delivery/) e no
Postgres (schema "delivery").
"""
import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark  # noqa: E402
from db import read_table, write_table  # noqa: E402

DELIVERY_OUT_DIR = os.environ.get("DELIVERY_OUT_DIR", "/opt/data/delivery")


def main():
    spark = get_spark("03_delivery")

    bancos = read_table(spark, "trusted", "bancos")
    reclamacoes = read_table(spark, "trusted", "reclamacoes")
    empregados = read_table(spark, "trusted", "empregados")

    reclamacoes_validas = reclamacoes.filter(F.col("cnpj").isNotNull())

    base = reclamacoes_validas.join(bancos, on="cnpj", how="inner")

    delivery = (
        base.join(
            empregados.select(
                "cnpj",
                *[c for c in empregados.columns if c not in ("cnpj",)],
            ),
            on="cnpj",
            how="left",
        )
        .withColumn(
            "possui_avaliacao_glassdoor",
            F.when(F.col("nome_normalizado").isNotNull() | F.col("origem_arquivo").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
        )
    )

    delivery.cache()

    os.makedirs(DELIVERY_OUT_DIR, exist_ok=True)
    delivery.write.mode("overwrite").parquet(
        os.path.join(DELIVERY_OUT_DIR, "tb_reclamacoes_bancos_funcionarios")
    )

    write_table(delivery, "delivery", "tb_reclamacoes_bancos_funcionarios")

    print("Delivery: tabela final gerada.")
    print(f"  tb_reclamacoes_bancos_funcionarios: {delivery.count()} linhas, {len(delivery.columns)} colunas")

    spark.stop()


if __name__ == "__main__":
    main()
