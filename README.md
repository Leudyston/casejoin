# Case Join - Processo Seletivo

Este projeto foi desenvolvido como parte do processo seletivo **Join**. Devido às limitações da conta gratuita do Databricks, não foi possível criar um **Secret Scope** para armazenar as credenciais de acesso ao banco de dados de exemplo. Uma alternativa seria utilizar o **Azure KeyVault** integrado ao Databricks.

## Estrutura do Processo ELT

O pipeline de ELT foi implementado em um único notebook no Databricks, dividido em quatro etapas principais:

### 1. Preparação

Nesta etapa, são importadas as bibliotecas necessárias para a execução do notebook, e os parâmetros usados ao longo do processo são definidos. Isso inclui:
- Configuração dos dados de conexão com o banco de dados.
- Definição da lista de tabelas que serão extraídas para a camada **Landing**.
- Criação dos esquemas para definir a arquitetura Medallion (Bronze, Silver, Gold).

> **Nota**: Para simplificar este case, as tabelas de análise foram criadas na camada **Silver**. No entanto, o propósito original dessa camada é anterior ao processo de análise.

### 2. Extração

Foi criada uma função para iterar sobre cada tabela na lista de extração. Após a execução, os dados de cada tabela são extraídos e armazenados na pasta **Landing** em subpastas nomeadas de acordo com as tabelas e no formato `.parquet`.

### 3. Carregamento

Nesta etapa, os dados extraídos são carregados na pasta **Bronze**, onde os arquivos são convertidos para o formato **Delta**. Em seguida, as tabelas são criadas no schema **Bronze** do Hive.

### 4. Transformação

Finalizando o processo, foram realizadas as análises solicitadas, utilizando **Spark SQL** e **PySpark**. Os resultados foram salvos no schema **Silver** para posterior análise e uso.

## Melhorias na Segurança

### 1. Mascaramento de Dados Sensíveis

Para proteger informações sensíveis, recomenda-se a integração com o **Unity Catalog**. Com ele, é possível criar políticas de mascaramento de dados, assegurando que apenas usuários autorizados possam acessar informações confidenciais.

### 2. Armazenamento Seguro no Data Lake

Os dados devem ser armazenados em um **Data Lake** com medidas de segurança avançadas, incluindo:
- **Criptografia** para proteger os dados armazenados.
- **Bloqueio de acesso público** para garantir que apenas usuários autorizados possam acessar os dados.

### 3. Controle de Acesso com Unity Catalog

O **Unity Catalog** permite um controle de acesso granular, possibilitando a criação de grupos de usuários e camadas de permissão específicas. Dessa forma, é possível controlar quem pode visualizar, modificar ou gerenciar os dados sem conceder acesso direto ao Data Lake.

## Monitoramento

Para melhorar o monitoramento e a gestão do ambiente no Databricks, as seguintes práticas são recomendadas:

### 1. Notificações e Alertas

- **Configuração de Notificações**:
  - Configure alertas automáticos por e-mail ou ferramentas como **Teams** e **Slack**.
  - Utilize **Databricks Jobs** para notificar sobre o status da execução dos notebooks (sucesso ou falha).
  - Considere o uso de **webhooks** para notificações personalizadas.

### 2. Relatórios de Desempenho

- **Monitoramento do Desempenho**:
  - Crie dashboards no **Databricks SQL Analytics** ou em ferramentas de BI para monitorar o tempo de execução dos notebooks.
  - Gere relatórios com métricas como o tempo total de execução e tempo gasto em cada etapa do pipeline para identificar possíveis gargalos.

### 3. Monitoramento do Crescimento de Dados

- **Crescimento de Dados no Data Lake**:
  - Implemente scripts para monitorar o crescimento dos dados no Data Lake.
  - Utilize ferramentas como **Azure Monitor** ou **AWS CloudWatch** para alertas sobre o uso de armazenamento e custos.
  - Configure relatórios periódicos para revisar o crescimento e tomar ações preventivas, como escalonamento ou otimização de armazenamento.

### 4. Monitoramento de Acessos às Tabelas

- **Acompanhamento de Acessos**:
  - Utilize ferramentas de auditoria para monitorar a frequência e a quantidade de acessos às tabelas.
  - Configure alertas para atividades incomuns e revise os logs regularmente.
  - Baseado nas informações coletadas, ajuste as políticas de atualização de tabelas, como reduzir a frequência de atualizações ou implementar atualizações sob demanda.

