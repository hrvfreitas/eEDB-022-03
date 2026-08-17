"""
Fuzzy matching de nomes de instituições, usado quando o cruzamento exato
por nome normalizado não encontra correspondência (sigla, pontuação,
acento ou ordem de palavras diferente).

Implementado com RapidFuzz, mas distribuído através do Spark usando
`mapInPandas`: cada partição do DataFrame Spark é processada em pandas
(com a tabela de nomes candidatos passada como broadcast), e o resultado
volta a ser um DataFrame Spark. Ou seja, o motor de execução continua
sendo o Spark — o pandas aqui é só a "célula" de trabalho de cada
partição, o mesmo padrão de um Pandas UDF.

Só aceita o match automaticamente com score >= 88 (0-100).
"""
import re
import unicodedata

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from rapidfuzz import process, fuzz

FUZZY_SCORE_THRESHOLD = 88


def normalizar_nome(nome: str) -> str:
    if nome is None:
        return ""
    nome = str(nome).upper().strip()
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = re.sub(r"[^A-Z0-9 ]", " ", nome)
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome


def fuzzy_match_dataframe(df, coluna_nome: str, candidatos: dict, spark):
    """
    df: Spark DataFrame contendo a coluna `coluna_nome` com nomes que NÃO
        tiveram match exato.
    candidatos: dict {nome_normalizado_candidato: cnpj} — tabela de bancos
        já tratada, para onde os nomes de `df` serão casados.
    Retorna um novo Spark DataFrame com as colunas extras:
        cnpj_fuzzy (string), fuzzy_score (int)
    """
    candidatos_bc = spark.sparkContext.broadcast(candidatos)

    out_schema = StructType(
        df.schema.fields
        + [
            StructField("cnpj_fuzzy", StringType(), True),
            StructField("fuzzy_score", IntegerType(), True),
        ]
    )

    def processar_particao(iterator):
        cand = candidatos_bc.value
        nomes_candidatos = list(cand.keys())
        for pdf in iterator:
            cnpjs, scores = [], []
            for nome in pdf[coluna_nome]:
                nome_norm = normalizar_nome(nome)
                if not nome_norm or not nomes_candidatos:
                    cnpjs.append(None)
                    scores.append(None)
                    continue
                match = process.extractOne(
                    nome_norm, nomes_candidatos, scorer=fuzz.token_sort_ratio
                )
                if match and match[1] >= FUZZY_SCORE_THRESHOLD:
                    cnpjs.append(cand[match[0]])
                    scores.append(int(match[1]))
                else:
                    cnpjs.append(None)
                    scores.append(None)
            pdf = pdf.copy()
            pdf["cnpj_fuzzy"] = cnpjs
            pdf["fuzzy_score"] = scores
            yield pdf

    return df.mapInPandas(processar_particao, schema=out_schema)
