# Databricks notebook source
# MAGIC %md
# MAGIC ## Arquivos e funções de apoio

# COMMAND ----------

# MAGIC %run /Eleicoes/utils/functions

# COMMAND ----------

from pyspark.sql.functions import input_file_name, col, split, element_at, substring_index, to_date, expr, from_unixtime, from_utc_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Paths utilizados

# COMMAND ----------

#paths
path_erp = "/FileStore/tables/eleicoes/erp/"
path_bronze = "/FileStore/tables/eleicoes/bronze/"

#files
file_consulta_cand = "consulta_cand/"
file_bem_cand = "bem_candidato/"

#variáveis para utilização da função fn_read_csv
encoding = "ISO-8859-1"
delimiter = ";"
header = "true"

#variáveis para utilização da função fn_write_parquet
mode = "overwrite"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria lista com o caminho dos arquivos

# COMMAND ----------

files_list = [file_consulta_cand, file_bem_cand]

# COMMAND ----------

for file in files_list:

    #Obtenção dos dados
    print("Obtendo dados dos arquivos: " + file)
    df = fn_read_csv(path_erp, file, delimiter, header, encoding)
    
    #Insere caminho do arquivo e local do candidato
    print("Inserindo identificação no dataframe de: " + file)
    df = fn_insert_filename(df)
    df = fn_insert_local_filename(df)
    
    print("Escrevendo o parquet do arquivo: " + file)
    #Escreve parquet em camada bronze
    fn_write_parquet(df, path_bronze, file, mode)
    
    print("")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Valida a escrita

# COMMAND ----------

df = fn_validate_folders(files_list, path_bronze)
df.display()
