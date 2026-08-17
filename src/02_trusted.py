"""
Etapa 2 — Camada Trusted

Lê a camada RAW (do Postgres) e aplica, via Spark DataFrame API (sem
nenhuma etapa em SQL):
  - tipagem correta das colunas
  - normalização da chave CNPJ (raiz, 8 dígitos, com zero-padding)
  - deduplicação de bancos por CNPJ
  - unificação e deduplicação dos dois arquivos de Empregados
  - resolução de CNPJ dos bancos "conglomerado" por nome (match exato +
    fallback via matching aproximado — fuzzy ou Splink, ver abaixo)

Grava o resultado em Parquet (data/trusted/) e no Postgres (schema
"trusted"), uma tabela por base.

## Estratégia de matching aproximado (flag MATCH_STRATEGY)

Para os nomes que não casam de forma exata com a base de Bancos, existem
duas estratégias alternativas de resolução, escolhidas pela variável de
ambiente `MATCH_STRATEGY` (ou `--match-strategy` na linha de comando):

  - `fuzzy`  (default) — RapidFuzz (token_sort_ratio), score mínimo 88.
             Implementado em `fuzzy_match.py`, distribuído via
             `DataFrame.mapInPandas`.
  - `splink` — matching probabilístico (Fellegi-Sunter, com EM) via
             Splink, usando o próprio backend Spark do Splink
             (`splink.spark.SparkAPI`). Implementado em `splink_match.py`.

Exemplos:
    MATCH_STRATEGY=fuzzy  python3 02_trusted.py
    MATCH_STRATEGY=splink python3 02_trusted.py
    python3 02_trusted.py --match-strategy splink
"""
import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(__file__))
from spark_session import get_spark  # noqa: E402
from db import read_table, write_table  # noqa: E402
from fuzzy_match import normalizar_nome  # noqa: E402

TRUSTED_OUT_DIR = os.environ.get("TRUSTED_OUT_DIR", "/opt/data/trusted")
MATCH_STRATEGIES = ("fuzzy", "splink")


def parse_match_strategy() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--match-strategy", choices=MATCH_STRATEGIES, default=None)
    args, _ = parser.parse_known_args()
    strategy = args.match_strategy or os.environ.get("MATCH_STRATEGY", "fuzzy")
    if strategy not in MATCH_STRATEGIES:
        raise ValueError(f"MATCH_STRATEGY inválida: {strategy!r}. Use uma de {MATCH_STRATEGIES}.")
    return strategy

normalizar_nome_udf = F.udf(normalizar_nome)


def cnpj_raiz_8(col):
    """Extrai/normaliza o CNPJ raiz para 8 dígitos com zero à esquerda."""
    digitos = F.regexp_replace(col.cast("string"), r"[^0-9]", "")
    return F.lpad(F.substring(digitos, 1, 8), 8, "0")


# ---------------------------------------------------------------- Bancos --
def tratar_bancos(spark):
    df = read_table(spark, "raw", "bancos")

    df = (
        df.withColumn("cnpj", cnpj_raiz_8(F.col("CNPJ")))
        .withColumn("nome_instituicao", F.trim(F.col("NomeInstituicao")))
        .withColumn("segmento_prudencial", F.trim(F.col("Segmento")))
        .withColumn("nome_normalizado", normalizar_nome_udf(F.col("nome_instituicao")))
    )

    # Prioriza o nome "- PRUDENCIAL" quando há 2 linhas para o mesmo CNPJ;
    # o outro nome é preservado em nome_alternativo.
    df = df.withColumn(
        "prioridade",
        F.when(F.upper(F.col("nome_instituicao")).contains("PRUDENCIAL"), F.lit(0)).otherwise(F.lit(1)),
    )
    w = Window.partitionBy("cnpj").orderBy(F.col("prioridade"))
    principal = (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    alternativo = (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 2)
        .select(F.col("cnpj").alias("cnpj_alt"), F.col("nome_instituicao").alias("nome_alternativo"))
    )

    trusted_bancos = (
        principal.join(alternativo, principal.cnpj == alternativo.cnpj_alt, "left")
        .select(
            "cnpj",
            "nome_instituicao",
            "nome_normalizado",
            "segmento_prudencial",
            "nome_alternativo",
        )
        .dropDuplicates(["cnpj"])
    )
    return trusted_bancos


# --------------------------------------------------------- Reclamações ----
def tratar_reclamacoes(spark, bancos_lookup: dict, spark_ctx, trusted_bancos_df, match_strategy: str):
    df = read_table(spark, "raw", "reclamacoes")

    df = (
        df.withColumn("cnpj_informado", F.col("CNPJ IF"))
        .withColumn(
            "cnpj",
            F.when(F.col("cnpj_informado").isNotNull() & (F.trim(F.col("cnpj_informado")) != ""),
                   cnpj_raiz_8(F.col("cnpj_informado"))),
        )
        .withColumn("nome_instituicao_reclamacao", F.trim(F.col("Instituição financeira")))
        .withColumn("nome_normalizado", normalizar_nome_udf(F.col("nome_instituicao_reclamacao")))
        .withColumn(
            "indice",
            F.regexp_replace(F.col("Índice"), ",", ".").cast("double"),
        )
        .withColumn("quantidade_total_reclamacoes", F.col("Quantidade total de reclamações").cast("int"))
        .withColumn(
            "trimestre",
            F.regexp_extract(F.col("arquivo_origem"), r"(\d{4}_tri_\d{2})", 1),
        )
    )

    # match exato pelo nome normalizado, para quem não tem CNPJ
    sem_cnpj = df.filter(F.col("cnpj").isNull())
    com_cnpj = df.filter(F.col("cnpj").isNotNull()).withColumn("cnpj_origem", F.lit("informado na fonte"))

    bancos_bc = spark_ctx.broadcast(bancos_lookup)

    def match_exato(nome_norm):
        return bancos_bc.value.get(nome_norm)

    match_exato_udf = F.udf(match_exato)

    sem_cnpj = sem_cnpj.withColumn("cnpj_match_exato", match_exato_udf(F.col("nome_normalizado")))

    resolvido_exato = sem_cnpj.filter(F.col("cnpj_match_exato").isNotNull()).withColumn(
        "cnpj", F.col("cnpj_match_exato")
    ).withColumn("cnpj_origem", F.lit("match exato"))

    pendente = sem_cnpj.filter(F.col("cnpj_match_exato").isNull()).drop("cnpj_match_exato")

    # fallback de matching aproximado para quem ainda não casou (fuzzy ou splink)
    if match_strategy == "fuzzy":
        from fuzzy_match import fuzzy_match_dataframe

        pendente_resolvido = fuzzy_match_dataframe(pendente, "nome_normalizado", bancos_lookup, spark)
        resolvido = pendente_resolvido.filter(F.col("cnpj_fuzzy").isNotNull()).withColumn(
            "cnpj", F.col("cnpj_fuzzy")
        ).withColumn(
            "cnpj_origem", F.concat(F.lit("fuzzy match, score="), F.col("fuzzy_score").cast("string"))
        ).drop("cnpj_fuzzy", "fuzzy_score")

        nao_resolvido = pendente_resolvido.filter(F.col("cnpj_fuzzy").isNull()).withColumn(
            "cnpj_origem", F.lit("nao resolvido")
        ).drop("cnpj_fuzzy", "fuzzy_score")

    elif match_strategy == "splink":
        from splink_match import splink_match_dataframe

        pendente_resolvido = splink_match_dataframe(
            pendente, "nome_instituicao_reclamacao",
            trusted_bancos_df, "nome_instituicao", "cnpj",
            spark,
        )
        resolvido = pendente_resolvido.filter(F.col("cnpj_splink").isNotNull()).withColumn(
            "cnpj", F.col("cnpj_splink")
        ).withColumn(
            "cnpj_origem",
            F.concat(F.lit("splink match, prob="), F.round(F.col("splink_match_probability"), 3).cast("string")),
        ).drop("cnpj_splink", "splink_match_probability")

        nao_resolvido = pendente_resolvido.filter(F.col("cnpj_splink").isNull()).withColumn(
            "cnpj_origem", F.lit("nao resolvido")
        ).drop("cnpj_splink", "splink_match_probability")

    else:
        raise ValueError(f"MATCH_STRATEGY desconhecida: {match_strategy!r}")

    cols_finais = [
        "cnpj", "cnpj_origem", "nome_instituicao_reclamacao", "trimestre",
        "indice", "quantidade_total_reclamacoes",
    ]

    trusted_reclamacoes = (
        com_cnpj.select(*cols_finais)
        .unionByName(resolvido_exato.select(*cols_finais))
        .unionByName(resolvido.select(*cols_finais))
        .unionByName(nao_resolvido.select(*cols_finais))
    )
    return trusted_reclamacoes


# ---------------------------------------------------------- Empregados ----
def tratar_empregados(spark):
    df_match = read_table(spark, "raw", "empregados_match")
    df_match_less = read_table(spark, "raw", "empregados_match_less")

    df_match_less = df_match_less.withColumn("cnpj", cnpj_raiz_8(F.col("CNPJ")))

    # arquivo "match" não traz CNPJ direto -> resolvido depois via bancos
    df_match = df_match.withColumn("nome_normalizado", normalizar_nome_udf(F.col("nome_empresa")))

    for c in ["nota_geral", "recomenda_empresa_pct"]:
        if c in df_match_less.columns:
            df_match_less = df_match_less.withColumn(c, F.col(c).cast("double"))

    df_match_less = df_match_less.withColumn("origem_arquivo", F.lit("match_less"))
    df_match = df_match.withColumn("origem_arquivo", F.lit("match"))

    # consolida por CNPJ, priorizando o registro do arquivo "match"
    unidos = df_match_less.unionByName(df_match, allowMissingColumns=True)
    w = Window.partitionBy("cnpj").orderBy(
        F.when(F.col("origem_arquivo") == "match", 0).otherwise(1)
    )
    trusted_empregados = (
        unidos.filter(F.col("cnpj").isNotNull())
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    return trusted_empregados


def main():
    match_strategy = parse_match_strategy()
    spark = get_spark(f"02_trusted[{match_strategy}]")
    sc = spark.sparkContext

    print(f"Estratégia de matching aproximado selecionada: {match_strategy}")

    trusted_bancos = tratar_bancos(spark)
    trusted_bancos.cache()

    bancos_lookup = {
        row["nome_normalizado"]: row["cnpj"]
        for row in trusted_bancos.select("nome_normalizado", "cnpj").collect()
    }

    trusted_reclamacoes = tratar_reclamacoes(spark, bancos_lookup, sc, trusted_bancos, match_strategy)
    trusted_empregados = tratar_empregados(spark)

    trusted_reclamacoes = trusted_reclamacoes.withColumn("match_strategy_usada", F.lit(match_strategy))

    os.makedirs(TRUSTED_OUT_DIR, exist_ok=True)
    trusted_bancos.write.mode("overwrite").parquet(os.path.join(TRUSTED_OUT_DIR, "bancos"))
    trusted_reclamacoes.write.mode("overwrite").parquet(
        os.path.join(TRUSTED_OUT_DIR, f"reclamacoes_{match_strategy}")
    )
    trusted_empregados.write.mode("overwrite").parquet(os.path.join(TRUSTED_OUT_DIR, "empregados"))

    write_table(trusted_bancos, "trusted", "bancos")
    # tabela "reclamacoes" é a que a Delivery consome (sempre a última rodada)
    write_table(trusted_reclamacoes, "trusted", "reclamacoes")
    # tabela extra, nomeada pela estratégia, útil para comparar fuzzy vs splink
    write_table(trusted_reclamacoes, "trusted", f"reclamacoes_{match_strategy}")
    write_table(trusted_empregados, "trusted", "empregados")

    print("Trusted: tratamento concluído.")
    print(f"  bancos:       {trusted_bancos.count()} linhas")
    print(f"  reclamacoes:  {trusted_reclamacoes.count()} linhas")
    print(f"  empregados:   {trusted_empregados.count()} linhas")

    spark.stop()


if __name__ == "__main__":
    main()
