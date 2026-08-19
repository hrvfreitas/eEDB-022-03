""" Etapa 3 — Camada Delivery
Lê as 3 tabelas Trusted (Postgres) e realiza, via Spark DataFrame API,
a união final por CNPJ, gerando a tabela:
delivery.tb_reclamacoes_bancos_funcionarios
Granularidade: banco x trimestre.
Grava em Parquet (data/delivery/) e no Postgres (schema "delivery").
"""
import os
import sys
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark
from db import read_table, write_table

DELIVERY_OUT_DIR = os.environ.get("DELIVERY_OUT_DIR", "/opt/data/delivery")

def main():
    spark = get_spark("03_delivery")

    # Lê as três tabelas Trusted
    bancos = read_table(spark, "trusted", "bancos")
    reclamacoes = read_table(spark, "trusted", "reclamacoes")
    empregados = read_table(spark, "trusted", "empregados")

    # Filtra apenas reclamações com CNPJ válido
    reclamacoes_validas = reclamacoes.filter(F.col("cnpj").isNotNull())

    # Join com bancos (inner, pois só interessam bancos conhecidos)
    base = reclamacoes_validas.join(bancos, on="cnpj", how="inner")

    # Join com empregados (left, pois nem todo banco tem avaliação Glassdoor)
    delivery = (base.join(
        empregados.select("cnpj", *[c for c in empregados.columns if c != "cnpj"]),
        on="cnpj",
        how="left",
    ).withColumn(
        "possui_avaliacao_glassdoor",
        F.when(
            F.col("nome_normalizado").isNotNull() | F.col("origem_arquivo").isNotNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    ))

    delivery.cache()  # cache para agilizar contagem e escrita

    # Cria diretório de saída
    os.makedirs(DELIVERY_OUT_DIR, exist_ok=True)

    # Escrita em Parquet
    delivery.write.mode("overwrite").parquet(
        os.path.join(DELIVERY_OUT_DIR, "tb_reclamacoes_bancos_funcionarios")
    )

    # Escrita no Postgres (schema delivery)
    write_table(delivery, "delivery", "tb_reclamacoes_bancos_funcionarios")

    print(f"Delivery: tabela final gerada. {delivery.count()} linhas, {len(delivery.columns)} colunas")
    spark.stop()

if __name__ == "__main__":
    main()
