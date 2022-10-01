# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

from pyspark.sql.functions import col, when, from_utc_timestamp, from_unixtime, regexp_replace

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Paths utilizados

# COMMAND ----------

#paths
path_bronze = "/FileStore/tables/eleicoes/bronze/"
path_silver = "/FileStore/tables/eleicoes/silver/"

#files
file_consulta_cand = "consulta_cand/"
file_bem_cand = "bem_candidato/"
files_list = [file_consulta_cand, file_bem_cand]

#variáveis para utilização da função fn_write_parquet
mode = "overwrite"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Leitura do arquivo

# COMMAND ----------

df = spark.read.parquet(path_bronze + file_bem_cand)
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Visualizando schema do arquivo

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Tratamentos 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1 - Substituindo #NULO#, #NULO, #NE, #NE# por null/none em todas as colunas

# COMMAND ----------

df = fn_replace_values_with_null(df, "#NULO#")
df = fn_replace_values_with_null(df, "#NULO")
df = fn_replace_values_with_null(df, "#NE")
df = fn_replace_values_with_null(df, "#NE#")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Substituindo , por . em na coluna VR_BEM_CANDIDATO

# COMMAND ----------

df = df.withColumn('VR_BEM_CANDIDATO', regexp_replace('VR_BEM_CANDIDATO', ',', '.').cast('double'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3 - Alterando tipagem das colunas

# COMMAND ----------

integer_columns =  [
                    'CD_ELEICAO',
                    'NR_ORDEM_CANDIDATO',
                    'CD_TIPO_BEM_CANDIDATO'
                    ]

df = fn_convert_multicolumn_type(df, integer_columns, 'integer')

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Gravando em camada silver

# COMMAND ----------

fn_write_parquet(df, path_silver, file_bem_cand, mode)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Valida a escrita

# COMMAND ----------

df = fn_validate_folders(files_list, path_silver)
df.display()
