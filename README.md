# casejoin
processo seletivo Join

Para esse Case, foi criado um unico notebook no databricks para executar o processo de ELT, devido a limitações na conta free, não foi possível criar um Secret Scope no Databricks para armanezar os dados de acesso ao banco de dados de exemplo, outra solução, seria usar uma KeyVault no Azure para vincular ao serviço do Databricks.

o Processo foi separado 4 etapas

- Preparação
  Nesta etapa as bibliotecas necessárias para execução do notebook são importadas, os parametros que serão usados são definidos, incluindo os dados para conexão com a base de dados e a lista das tabelas que serão extraídas para Landing, por fim, os esquemas para definição da arquitetura Medallion são criados.
  OBS: nesse case foi usada a propria camada silver para criar as tabelas das analises, porém o obajetivo dessa camada é outro, um anterior a esse processo.
   
- Extração
  Nessa estapa criou-se uma função para iterar com cada tabela da lista criada anteriormente, que após executada terá extraido e armazenados os dados de cada tabela na pasta Landing e subpastas com nomes de cada tabela e com arquivos no formato .parquet.
  
- Carregamento
  Nessa etapa, seguimos com o carregamento dos dados para a pasta Bronze transformando os arquivos para o formato delta, e em seguida criando as tabelas no schema Bronze do Hive.
  
- Transformação
  Nessa etapa, finalizamos o processo criando as análises solicitadas usando linguagens como spark sql e pyspark para criar os scripts e salvar os resultados na schema silver.
