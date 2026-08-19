""" Fuzzy matching de nomes de instituições, usado quando o cruzamento exato
por nome normalizado não encontra correspondência.
Implementado com RapidFuzz, distribuído via Spark usando mapInPandas.
"""
import re
import unicodedata
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from rapidfuzz import process, fuzz

FUZZY_SCORE_THRESHOLD = 88  # score mínimo para aceitar um match

def normalizar_nome(nome: str) -> str:
    """Normaliza nome: maiúsculas, remove acentos, remove pontuação, espaços extras."""
    if nome is None:
        return ""
    nome = str(nome).upper().strip()
    nome = unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("ASCII")
    nome = re.sub(r"[^A-Z0-9 ]", " ", nome)  # mantém letras, números e espaços
    nome = re.sub(r"\s+", " ", nome).strip()
    return nome

def fuzzy_match_dataframe(df, coluna_nome: str, candidatos: dict, spark):
    """
    df: Spark DataFrame com nomes que NÃO tiveram match exato.
    candidatos: dict {nome_normalizado_candidato: cnpj} — tabela de bancos.
    Retorna um novo Spark DataFrame com as colunas extras: cnpj_fuzzy, fuzzy_score.
    """
    # Transmite o dicionário de candidatos para todas as partições (broadcast)
    candidatos_bc = spark.sparkContext.broadcast(candidatos)

    # Define o schema de saída (colunas originais + duas novas)
    out_schema = StructType(
        df.schema.fields + [
            StructField("cnpj_fuzzy", StringType(), True),
            StructField("fuzzy_score", IntegerType(), True),
        ]
    )

    def processar_particao(iterator):
        """Função executada em cada partição (pandas iterador)."""
        cand = candidatos_bc.value
        nomes_candidatos = list(cand.keys())
        for pdf in iterator:  # pdf é um pandas DataFrame
            cnpjs, scores = [], []
            for nome in pdf[coluna_nome]:
                nome_norm = normalizar_nome(nome)
                if not nome_norm or not nomes_candidatos:
                    cnpjs.append(None)
                    scores.append(None)
                    continue
                # Busca o melhor candidato com token_sort_ratio (ignora ordem das palavras)
                match = process.extractOne(nome_norm, nomes_candidatos, scorer=fuzz.token_sort_ratio)
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

    # Aplica a função em cada partição (mapInPandas)
    return df.mapInPandas(processar_particao, schema=out_schema)
