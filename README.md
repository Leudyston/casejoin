# Case Join
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


## Melhorias na Segurança

### 1. Mascaramento de Dados Sensíveis

Se os dados contiverem informações sensíveis, é recomendável associá-los ao **Unity Catalog**. Utilizando o Unity Catalog, você pode criar funções para realizar o mascaramento dos dados, garantindo que informações confidenciais sejam protegidas e visíveis apenas para usuários autorizados.

### 2. Armazenamento Seguro em Data Lake

Salve os dados em um **Data Lake** com medidas de segurança robustas. Isso inclui o uso de **criptografia** para proteger os dados armazenados e o **bloqueio de acesso público** para garantir que apenas usuários autorizados possam acessar as informações.

### 3. Controle de Acesso Refinado com Unity Catalog

O **Unity Catalog** oferece um controle de acesso avançado, permitindo a criação de **grupos de usuários** e **camadas de permissão específicas**. Dessa forma, é possível definir precisamente quem pode visualizar, modificar ou gerenciar os dados, sem a necessidade de conceder acesso direto ao Data Lake.


## Monitoramento
Para aprimorar o monitoramento e a gestão dos notebooks e dados no Databricks, considere implementar as seguintes medidas:

### 1. Notificações e Alertas

- **Configurar Disparos de E-mail e Notificações em Ferramentas de Comunicação:**
  - Configure alertas automáticos por e-mail ou integração com ferramentas como **Teams**, **Slack**, ou outras plataformas de comunicação.
  - Utilize **Databricks Jobs** para definir notificações baseadas no status da execução dos notebooks, como falhas ou conclusão bem-sucedida.
  - Considere o uso de **webhooks** para enviar notificações personalizadas.

### 2. Relatórios de Desempenho

- **Gerar Relatórios de Desempenho Detalhados:**
  - Crie dashboards no **Databricks SQL Analytics** ou utilize ferramentas de BI para gerar relatórios sobre o tempo de execução dos notebooks.
  - Inclua métricas como tempo total de execução, tempo gasto em cada etapa do pipeline, e tendências ao longo do tempo para identificar gargalos e otimizar processos.

### 3. Monitoramento de Crescimento de Dados

- **Acompanhar o Crescimento dos Dados no Data Lake:**
  - Implemente scripts de monitoramento para rastrear o crescimento dos dados no Data Lake.
  - Utilize **Azure Monitor** ou **AWS CloudWatch** (dependendo da plataforma) para configurar alertas sobre o uso de armazenamento e custos.
  - Configure relatórios periódicos para revisar o crescimento e planejar medidas preventivas, como escalonamento de capacidade ou otimização de armazenamento.

### 4. Monitoramento de Acessos às Tabelas

- **Monitorar Acessos e Frequência:**
  - Utilize ferramentas de auditoria e logs para monitorar a quantidade e a frequência de acessos às tabelas.
  - Configure alertas para atividades inesperadas e revise os logs periodicamente.
  - Baseado nos dados coletados, tome decisões sobre a atualização da tabela, como reduzir a frequência de atualizações ou configurar atualizações sob demanda.
