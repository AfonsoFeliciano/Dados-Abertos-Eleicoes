# Databricks notebook source
from pyspark.sql.functions import input_file_name, col, when

# COMMAND ----------

#Função para realizar leitura de arquivos csv 
def fn_read_csv(path, file, delimiter, header, encoding):
    df = spark.read.format("csv").option("delimiter", delimiter).option("header", header).option("encoding", encoding).load(path + file)
    return df

# COMMAND ----------

#Função para inserir o nome do arquivo no dataframe
def fn_insert_filename(df):
    df = df.withColumn("FILENAME", input_file_name())
    return df

# COMMAND ----------

#Função para extração dos locais presentes nos nomes dos arquivos
def fn_insert_local_filename(df):
    df = df.withColumn('LOCAL', split(df['FILENAME'], '_')) \
           .withColumn('LOCAL', substring_index(element_at(col('LOCAL'), -1), ".", 1) )
    return df

# COMMAND ----------

#Função para escrita de arquivos parquets
def fn_write_parquet(df, path, file, mode):
    df.write.mode(mode).parquet(path + file)

# COMMAND ----------

#Função para substituir valores indesejados por null em todas as colunas
def fn_replace_values_with_null(df, value):
    df = df.select( [ when(col(c) == value, None).otherwise(col(c)).alias(c) for c in df.columns ] )
    return df

# COMMAND ----------

#Função para conversão de tipos em mais de uma coluna utilizando list comprehension
def fn_convert_multicolumn_type(df, columns, type):
    df = df.select(* ( c for c in df.columns if c not in columns ), * ( col(c).cast(type).alias(c) for c in columns ))
    return df

# COMMAND ----------

#Função para visualização de folders em dbfs após escritas de arquivos
def fn_validate_folders(files_list, path):
    
    #Cria lista com todos os caminhos de determinados diretorios
    for file in files_list:
    
        lists = []

        final_path = path_bronze + file
        lists.append( dbutils.fs.ls(final_path) )

    #Transforma em lista
    lists = [item for sublist in lists for item in sublist]

    #Cria dataframe com as colunas presentes
    columns = ["path", "size", "name", "modificationTime"]
    df = spark.createDataFrame(lists, columns)
    
    #Converte a coluna modificationTime de timestamp milessegundos para timestamp segundos
    #3 - Converte timestamp segundos para date
    #4 - Altera o GMT para GMT-03 America/SaoPaulo
    df = df.withColumn("modificationTime", from_utc_timestamp(from_unixtime(col("modificationTime") / 1000, "yyyy-MM-dd HH:mm:ss"), "America/Sao_Paulo"))
    
    return df