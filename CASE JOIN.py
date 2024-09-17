# Databricks notebook source
# MAGIC %md
# MAGIC ## Preparação

# COMMAND ----------

# DBTITLE 1,Importar Bibliotecas
import os
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Definir Parâmetros para Execução do Notebook
# Caminho onde os arquivos .parquet foram salvos
landing_path = "/join/landing/"

# Caminho para salvar os arquivos no formato Delta
bronze_path = "/join/bronze/"

# Configurações de conexão com Azure PostgreSQL
jdbc_url = "jdbc:postgresql://psql-mock-database-cloud.postgres.database.azure.com:5432/ecom1692155331663giqokzaqmuqlogbu"
jdbc_properties = {
    "user": "eolowynayhvayxbhluzaqxfp@psql-mock-database-cloud",
    "password": "hdzvzutlssuozdonhflhwyjm",
    "driver": "org.postgresql.Driver"
}


# COMMAND ----------

# DBTITLE 1,Criar de Schemas
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Identificar e criar lista de tabelas a serem extraidas
# Consulta para obter a lista de tabelas
list_tables_query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT IN ('pg_stat_statements','pg_buffercache')) AS tables"

# Ler as tabelas disponíveis
tables_df = spark.read.jdbc(url=jdbc_url, table=list_tables_query, properties=jdbc_properties)
tables = tables_df.select("table_name").rdd.flatMap(lambda x: x).collect()

# Exibir tabelas encontradas
print("Tabelas encontradas:", tables)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração do Dados

# COMMAND ----------

# DBTITLE 1,Extrair dados das tabelas e Salvando na Landing
# Iterar sobre as tabelas e salvar cada uma como arquivo .parquet na landing
for table in tables:
    # Definir o caminho de destino para o arquivo parquet
    table_path = os.path.join(landing_path, f"{table}")
    
    # Ler os dados da tabela
    table_df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_properties)
    
    # Salvar os dados como .parquet
    table_df.write.mode("overwrite").parquet(table_path)
    
    # Mostrar um exemplo dos dados da tabela
    # print(f"Exibindo exemplo de dados para a tabela '{table}':")
    # table_df.show(5, truncate=False)  # Exibe as primeiras 5 linhas, não truncando o texto
    
    print(f"Tabela '{table}' salva em {table_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos Dados

# COMMAND ----------

# DBTITLE 1,Criar arquivos na Camada Bronze
# Função para ler parquet, salvar em Delta na bronze.
def convert_parquet_to_delta(table_name):
    # Caminho para os arquivos .parquet e o destino Delta
    parquet_path = os.path.join(landing_path, f"{table_name}")
    delta_path = os.path.join(bronze_path, table_name)
    
    # Ler os dados do arquivo parquet
    df = spark.read.parquet(f"{parquet_path}")
    
    # Escrever os dados no formato Delta
    df.write.format("delta").mode("overwrite").save(f"{delta_path}")

    # Ler e mostrar exemplos dos dados da tabela Delta
    # print(f"Exibindo exemplo de dados para a tabela '{table_name}':")
    # delta_df = spark.read.format("delta").load(f"{delta_path}")
    # delta_df.show(5, truncate=False)  # Exibe as primeiras 5 linhas, não truncando o texto
    
    print(f"Tabela '{table_name}' salva em Delta em {delta_path}")

# Iterar sobre todas as tabelas e convertê-las
for table in tables:
    convert_parquet_to_delta(table)



# COMMAND ----------

# DBTITLE 1,Criar Tabelas Bronze no Hive
# Função para criar uma tabela Delta no schema bronze
def create_delta_table(table_name):
    delta_path = os.path.join(bronze_path, table_name)
    
    # SQL para criar uma tabela Delta apontando para o caminho Delta
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS bronze.{table_name}
    USING DELTA
    LOCATION '{delta_path}'
    """
    
    # Executar o comando SQL para criar a tabela Delta
    spark.sql(create_table_sql)
    
    # Ler e mostrar exemplos dos dados da tabela Delta
    # print(f"Exibindo exemplo de dados para a tabela Delta '{table_name}':")
    # delta_df = spark.read.format("delta").load(delta_path)
    # delta_df.show(5, truncate=False)  # Exibe as primeiras 5 linhas, não truncando o texto
    
    print(f"Tabela Delta '{table_name}' criada no schema bronze em {delta_path}")

# Iterar sobre todas as tabelas e criar as tabelas Delta
for table in tables:
    create_delta_table(table)


# COMMAND ----------

# MAGIC %md
# MAGIC #Tranformação dos Dados

# COMMAND ----------

# DBTITLE 1,País com maior quantidade de Ordens Canceladas
# Criar DataFrame com valores da consulta
ordens_canceladas = spark.sql("""
SELECT c.country as pais, COUNT(DISTINCT o.order_number) AS qtd_ordens_canceladas
FROM bronze.orders o 
INNER JOIN bronze.customers c ON o.customer_number = c.customer_number
WHERE o.status = 'Cancelled'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""")

# Mostrar Valores
ordens_canceladas.show()

# Salvar o DataFrame no schema silver como uma tabela Hive
ordens_canceladas.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.ordens_canceladas")

print(f"Tabela Delta ordens_canceladas criada no schema silver")


# COMMAND ----------

# DBTITLE 1,País com mais quantidade de itens cancelados
# Criar DataFrame com valores da consulta
itens_cancelados = spark.sql("""
select c.country as pais, SUM(od.quantity_ordered) qtd_canceladas
from bronze.orders o 
inner join bronze.customers c on o.customer_number = c.customer_number
inner join bronze.orderdetails od on o.order_number = od.order_number
where status = 'Cancelled'
group by 1
order by 2 desc
limit 1
""")

# Mostrar Valores
itens_cancelados.show()

# Salvar o DataFrame no schema silver como uma tabela Hive
itens_cancelados.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.itens_cancelados")

print(f"Tabela Delta itens_cancelados criada no schema silver")

# COMMAND ----------

# DBTITLE 1,Faturamento da Linha de Produto Mais Vendida
# Criar DataFrame com valores da consulta
faturamento_linha = spark.sql("""
    select pl.product_line as linha_de_produto, SUM(od.quantity_ordered) as Quantidade, SUM(od.quantity_ordered * od.price_each) as Faturamento
    from bronze.orders o 
    inner join bronze.orderdetails od on o.order_number = od.order_number
    inner join bronze.products p on od.product_code = p.product_code
    inner join bronze.product_lines pl on p.product_line=pl.product_line
    where o.status = 'Shipped'
    AND year(o.order_date) = 2005 
    group by 1
    order by SUM(od.quantity_ordered) desc
    limit 1
    """)

# Mostrar Valores
faturamento_linha.show()

# Salvar o DataFrame no schema silver como uma tabela Hive
faturamento_linha.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.faturamento_linha")

print(f"Tabela Delta faturamento_linha criada no schema silver")    

# COMMAND ----------

# DBTITLE 1,Vendedores do Japão com email mascarado
# Carregar o DataFrame
df = spark.table("bronze.employees").alias("e") \
    .join(spark.table("bronze.offices").alias("o"), F.col("e.office_code") == F.col("o.office_code")) \
    .filter(F.col("o.country") == "Japan") \
    .filter(F.col("job_Title") == "Sales Rep" )

# Mascara o local-part do e-mail
df_masked = df.withColumn(
    "masked_email",
    F.concat(
        F.lit("xxxxx"),
        F.substring(F.col("e.email"), F.instr(F.col("e.email"), "@"), F.length(F.col("e.email")))
    )
).select(
    F.col("e.first_name").alias("nome"),
    F.col("e.last_name").alias("sobrenome"),
    F.col("masked_email")
)

# Mostrar Valores
df_masked.show(100)

# Salvar o DataFrame no schema silver como uma tabela Hive
df_masked.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.vendedores")


print(f"Tabela Delta vendedores criada no schema silver")  
