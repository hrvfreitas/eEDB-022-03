"""
Etapa 1 — Camada RAW

Lê as fontes originais (Reclamações, Bancos, Empregados) EXATAMENTE como
elas chegam (mesmos nomes de coluna, tudo como string, sem nenhum
tratamento), usando Spark, e grava:

  1) em disco, como espelho em Parquet dentro de data/raw/  (formato de
     armazenamento livre — usamos Parquet aqui só para ter um formato
     colunar rápido de reler, mas o conteúdo continua "cru")
  2) no Postgres, schema "raw", uma tabela por fonte.


"""
import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark  # noqa: E402
from db import write_table  # noqa: E402

RAW_ORIGEM_DIR = os.environ.get("RAW_ORIGEM_DIR", "/opt/data/raw/origem")
RAW_OUT_DIR = os.environ.get("RAW_OUT_DIR", "/opt/data/raw")


def read_csv_all_string(spark, path, sep, encoding):
    """Lê um CSV (ou pasta de CSVs) com todas as colunas como string,
    preservando o dado exatamente como veio da fonte."""
    return (
        spark.read.option("header", True)
        .option("sep", sep)
        .option("encoding", encoding)
        .option("inferSchema", False)
        .csv(path)
    )


def ingest_reclamacoes(spark):
    """8 arquivos trimestrais de reclamações (';' , Latin-1).
    O arquivo vazio 2022_tri_02_nao_ha_dados.csv é ignorado automaticamente
    pois o próprio Spark descarta arquivos sem linhas de dados."""
    path = os.path.join(RAW_ORIGEM_DIR, "Reclamações", "*.csv")
    df = read_csv_all_string(spark, path, sep=";", encoding="ISO-8859-1")
    df = df.withColumn("arquivo_origem", F.input_file_name())
    return df


def ingest_bancos(spark):
    """Enquadramento dos bancos ('\\t' , Latin-1)."""
    path = os.path.join(RAW_ORIGEM_DIR, "Bancos", "EnquadramentoInicia_v2.tsv")
    df = read_csv_all_string(spark, path, sep="\t", encoding="ISO-8859-1")
    return df


def ingest_empregados(spark):
    """Avaliações Glassdoor: dois arquivos ('|' , UTF-8), unidos apenas na
    camada RAW por concatenação simples (sem deduplicação — isso é
    tratamento e acontece na Trusted)."""
    base = os.path.join(RAW_ORIGEM_DIR, "Empregados")
    df_match = read_csv_all_string(
        spark, os.path.join(base, "glassdoor_consolidado_join_match_v2.csv"),
        sep="|", encoding="UTF-8",
    ).withColumn("arquivo_origem", F.lit("match"))
    df_match_less = read_csv_all_string(
        spark, os.path.join(base, "glassdoor_consolidado_join_match_less_v2.csv"),
        sep="|", encoding="UTF-8",
    ).withColumn("arquivo_origem", F.lit("match_less"))
    return df_match, df_match_less


def main():
    spark = get_spark("01_ingest_raw")

    os.makedirs(RAW_OUT_DIR, exist_ok=True)

    df_reclamacoes = ingest_reclamacoes(spark)
    df_bancos = ingest_bancos(spark)
    df_emp_match, df_emp_match_less = ingest_empregados(spark)

    # ---- disco (camada RAW em disco, formato livre -> Parquet) ----
    df_reclamacoes.write.mode("overwrite").parquet(os.path.join(RAW_OUT_DIR, "reclamacoes"))
    df_bancos.write.mode("overwrite").parquet(os.path.join(RAW_OUT_DIR, "bancos"))
    df_emp_match.write.mode("overwrite").parquet(os.path.join(RAW_OUT_DIR, "empregados_match"))
    df_emp_match_less.write.mode("overwrite").parquet(os.path.join(RAW_OUT_DIR, "empregados_match_less"))

    # ---- Postgres, schema raw ----
    write_table(df_reclamacoes, "raw", "reclamacoes")
    write_table(df_bancos, "raw", "bancos")
    write_table(df_emp_match, "raw", "empregados_match")
    write_table(df_emp_match_less, "raw", "empregados_match_less")

    print("RAW: ingestão concluída.")
    print(f"  reclamacoes:          {df_reclamacoes.count()} linhas")
    print(f"  bancos:                {df_bancos.count()} linhas")
    print(f"  empregados_match:      {df_emp_match.count()} linhas")
    print(f"  empregados_match_less: {df_emp_match_less.count()} linhas")

    spark.stop()


if __name__ == "__main__":
    main()
