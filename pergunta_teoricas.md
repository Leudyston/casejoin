## 1. Como você utiliza o Delta Lake no Azure Databricks para garantir a integridade dos dados?

Uso algumas técnicas chave. Primeiro, aplico transações ACID para manter a consistência dos dados. Também utilizo o controle de versionamento para rastrear e reverter mudanças se necessário. 

## 2. Quais são as vantagens do uso do Spark em comparação com outras tecnologias de processamento de dados?

Principal vantagem e ele ser rápido e flexível. Ele processa dados de forma eficiente porque mantém tudo na memória, o que acelera o trabalho. Pode lidar com vários tipos de dados e oferece uma interface única para fazer análises, aprendizado de máquina e processamento em tempo real.

## 3. Descreva um caso em que você precisou sincronizar dados entre diferentes sistemas.

Em um grande cliente de varejo, tínhamos dados provenientes de OMS e Linx. Os dados do OMS eram de vendas de apps e sites, enquanto os do Linx vinham das lojas físicas. Devido às diferenças de fuso horário entre as bases de dados, era necessário sincronizar as cargas e o processamento dos dados para um único fuso. Para isso, criamos funções que ajustavam as colunas de datas de acordo com o fuso horário correto.

## 4. Desenhe uma arquitetura de dados comentada para uma empresa que utiliza Azure e Databricks, incluindo armazenamento, processamento e análise.

### Coleta de Dados
- **Data Sources -> Azure Data Factory**: Utilizamos o Azure Data Factory para movimentar e integrar dados de diferentes fontes para um local centralizado.

### Armazenamento
- **Dados Brutos e Dados Processados -> Azure Blob Storage/Azure Data Lake Store**: O Blob Storage é econômico e adequado para armazenar dados não estruturados e Data Lake Store para dados estruturados.

### Processamento
- **Databricks Notebooks**
- **Delta Live Tables (DLT)**: Definimos e executamos pipelines ETL de forma simplificada e gerenciada. O DLT facilita a ingestão e transformação de dados, garantindo a qualidade com verificações e validações integradas.

### Análise de Dados
- **Power BI**: Para análises com dashboards e relatórios.
- **Databricks Notebooks**: Para análises mais avançadas.
- **Databricks SQL Analytics**: Para análise interna de processos, armazenamento e custos.

### Governança e Segurança
- **Unity Catalog**: Gerencia metadados e aplica políticas de segurança e controle de acesso em tabelas Delta, além de garantir a linhagem dos dados automaticamente.
- **Databricks Access Control**
- **Databricks Audit Logs**
- **Azure Active Directory (AAD)**
- **Azure RBAC**

### CI/CD e Versionamento
- **DevOps para Versionamento de Código**: Versionamos notebooks e pipelines.
- **DevOps para Pipeline de CI/CD**: Automatizamos a integração e o deploy dos notebooks e pipelines do Databricks.

## 5. Como você garante a escalabilidade e a robustez da arquitetura de dados?

Uso armazenamento e processamento distribuído, particionamento e otimização de consultas, uso o SHUFFLE e o AUTO OPTIMIZE. Isso ajuda a gerenciar grandes volumes de dados de forma eficiente. Para assegurar a robustez, trago novamente as transações ACID e configuro alertas para detectar problemas rapidamente. Tento seguir sempre boas práticas.

## 6. Como você implementa a criptografia de dados em repouso e em trânsito?

Para dados em repouso, o Azure Blob Storage oferece criptografia nativa. Para dados em trânsito, utilizo TLS/SSL para criptografar as conexões. Se for via API, asseguro que as conexões sejam seguras (HTTPS).

## 7. Como você gerencia a qualidade dos dados em um pipeline de dados?

No Delta Live Tables (DLT), é possível criar códigos adicionais nos notebooks para automatizar o processo de validação e garantir a qualidade dos dados.

## 8. Qual a importância do FinOps para a engenharia de dados?

Controlar e prever custos, otimizar recursos e garantir que o investimento em dados esteja alinhado com a estratégia e as metas da empresa.

## 9. Como o DevOps ajuda o engenheiro de dados?

Uso de pipelines CI/CD, o versionamento de código com integração do Git, a colaboração entre equipes e a automação de processos repetitivos.

## 10. Como iniciamos um projeto de pipeline de dados?

Começamos refinando as ações necessárias, identificando e definindo a entrega de valor, estabelecendo requisitos e identificando riscos.

## 11. Como realizar CI/CD em um pipeline de dados?

Automatizo processos que incluem testes, integração e deployment. Configuro o repositório de código, implemento a integração contínua (CI), a entrega contínua (CD), e monitoro e gerencio versões e rollbacks. Tento sempre manter a documentação clara e atualizada.

## 12. Quais ferramentas de orquestração você já trabalhou?

Trabalhei com Databricks, Azure Data Factory e Synapse Analytics.

## 13. Quais suas motivações para ser um engenheiro de dados?

Trabalho com dados desde 2006, mas foquei em inteligência de dados a partir de 2017. O que me motiva é saber que meu trabalho automatiza processos demorados e caros, que as informações geradas ajudam empresas a tomar decisões mais assertivas sem achismo. Além disso, adoro tecnologia e tenho prazer em desenvolver códigos cada vez mais performáticos. Ver o impacto positivo das soluções que crio, reduzindo custos e ajudando no dia a dia das empresas, me estimula a sempre fazer o meu melhor.
