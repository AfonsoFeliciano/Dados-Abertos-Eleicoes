# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

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
# MAGIC ## Realizando leitura e criando DM_TIPO_BEM_CANDIDATO

# COMMAND ----------

df = spark.read.parquet(path_silver + file_bem_cand).select(

"CD_ELEICAO",
"CD_TIPO_ELEICAO", 
"DT_ELEICAO", 
"SG_UF",
"SG_UE",
"SQ_CANDIDATO",
"CD_TIPO_BEM_CANDIDATO",
"DT_ULTIMA_ATUALIZACAO",
"HH_ULTIMA_ATUALIZACAO",
"NR_ORDEM_CANDIDATO",
"VR_BEM_CANDIDATO"



).distinct()
df.display()
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada gold

# COMMAND ----------

fn_write_parquet(df, path_gold_fatos, "fato_patrimonio_candidato", mode)

# COMMAND ----------

