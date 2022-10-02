# Dados-Abertos-Eleicoes
Repositório para processamento e modelagem dimensional dos dados das eleições utilizando Spark no Databricks Community


## Sobre as camadas

### ERP

A camada ERP é meramente ilustrativa, utilizada apenas para armazenar os arquivos em seu formato original no qual foram baixados do portal de dados abertos. 

### Bronze

Em camada bronze, os arquivos foram lidos e unidos e salvos em arquivo colunar, neste caso parquet com o objetivo de utilizar sua compressão.

### Silver

Na camada silver os dados foram limpados e realizados os mais diversos tratamentos para torná-los confiáveis para consumo.

### Gold

Por fim, em camada gold, os dados foram modelados utilizando modelagem dimensional, fisicalizando tabelas fatos e dimensões. 

