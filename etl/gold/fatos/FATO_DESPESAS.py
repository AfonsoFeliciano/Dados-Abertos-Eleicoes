# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, to_date, date_format

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Paths utilizados

# COMMAND ----------

#paths
path_silver = "/FileStore/tables/eleicoes/silver/"
path_gold_dm = "/FileStore/tables/eleicoes/gold/dimensoes/"
path_gold_fatos = "/FileStore/tables/eleicoes/gold/fatos/"

#files
file_consulta_cand = "consulta_cand/"
file_bem_cand = "bem_candidato/"
files_list = [file_consulta_cand, file_bem_cand]

#variáveis para utilização da função fn_write_parquet
mode = "overwrite"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Realizando leitura e criando FATO_DESPESAS

# COMMAND ----------

df = spark.read.parquet(path_silver + file_consulta_cand).select(

date_format(to_date(col("DT_ELEICAO"), "dd/MM/yyyy"), "yyyyMMdd").cast("int").alias("NR_DT_ELEICAO"),
"CD_TIPO_ELEICAO", 
"CD_ELEICAO",
"SQ_CANDIDATO",
"CD_CARGO",
"SQ_COLIGACAO",
"CD_COR_RACA",
"CD_ESTADO_CIVIL",
"NR_FEDERACAO",
"CD_GENERO",
"CD_GRAU_INSTRUCAO",
"CD_NACIONALIDADE",
"CD_OCUPACAO",
"NR_PARTIDO",
"CD_SITUACAO_CANDIDATURA",
"ST_DECLARAR_BENS",
"ST_PREST_CONTAS",
"SG_UF", 
"SG_UE",
"VR_DESPESA_MAX_CAMPANHA"

).distinct()

df.display()
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada gold

# COMMAND ----------

fn_write_parquet(df, path_gold_fatos, "fato_despesas", mode)