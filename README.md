# Local Data Stack

Stack local de engenharia de dados com Apache Airflow, dbt, Airbyte (Cloud), Snowflake e Metabase, empacotada com Docker.

## Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Tecnologias](#tecnologias)
- [Pré-requisitos](#pré-requisitos)
- [Configuração Rápida](#configuração-rápida)
- [Variáveis de Ambiente](#variáveis-de-ambiente)
- [Execução](#execução)
- [Serviços e Acessos](#serviços-e-acessos)
- [Configuração de Integrações](#configuração-de-integrações)
    - [Airflow → Snowflake](#airflow--snowflake)
    - [Airflow → Airbyte Cloud](#airflow--airbyte-cloud)
    - [Metabase](#metabase)
- [DAGs Disponíveis](#dags-disponíveis)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Desenvolvimento](#desenvolvimento)

## Visão Geral

Este repositório fornece uma stack moderna de dados para:
- Ingestão de dados com Airbyte (Cloud).
- Orquestração de pipelines com Apache Airflow.
- Transformações com dbt (integrado ao Airflow via Cosmos).
- Visualização e exploração com Metabase.
- Armazenamento e processamento no Snowflake.

Caso de uso principal: ingestão e transformação de dados de geração de usinas da ONS (Operador Nacional do Sistema Elétrico) para posterior análise.

## Arquitetura

Fluxo de alto nível:

1. Airbyte Cloud lê arquivos Parquet públicos da ONS.
2. Airbyte carrega os dados no Snowflake.
3. Airflow orquestra ingestão (airbyte) e transformação (dbt).
4. Metabase se conecta ao Snowflake para visualização.

## Tecnologias

- Apache Airflow 2.x
- Astronomer Cosmos (integração Airflow + dbt)
- dbt
- Airbyte Cloud
- Snowflake
- Metabase
- Docker e Docker Compose
- Python 3.12

## Pré-requisitos

- Docker e Docker Compose instalados
- Conta no Snowflake (trial funciona)
- Conta no Airbyte Cloud (para ingestão gerenciada)
- Git instalado
- Recomendado: ao menos 4 CPUs e 4 GB RAM disponíveis para containers

## Configuração Rápida

1. Clone o repositório:
   ```bash
   git clone git@github.com:alexpereiramaranhao/local-data-stack.git
   cd local-data-stack
   ```

2. Crie o arquivo de variáveis de ambiente:
   ```bash
   cp .env_example .env
   ```

3. Edite o `.env` e preencha:
    - Credenciais/segredo do Airflow.
    - Connection ID e API Key do Airbyte Cloud (se usar).
    - Período de ingestão dos dados ONS (se aplicável).

4. Suba os serviços:
    - Com Astro CLI (se usa Astronomer localmente):
      ```bash
      astro dev start
      ```

5. Acesse as interfaces:
    - Airflow: http://localhost:8080 (padrão admin/admin)
    - Metabase: http://localhost:3000

## Variáveis de Ambiente

Edite o arquivo `.env` para ajustar as variáveis usadas pelos DAGs e conexões.

Exemplos de variáveis esperadas:
- Segredo do Airflow Webserver:
  ```
  AIRFLOW__WEBSERVER__SECRET_KEY="sua-chave-bem-aleatoria"
  ```
- Integração Airbyte Cloud:
  ```
  AIRBYTE_API_BASE_URL="https://api.airbyte.com/v1"
  AIRBYTE_CONNECTION_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  AIRBYTE_API_KEY="sua-api-key"
  ```
- Janela de ingestão dos dados ONS:
  ```
  START_DATE_ONS_DATA_INGESTION="2025/01/01"
  END_DATE_ONS_DATA_INGESTION="2025/12/31"
  ```

Observação: variáveis sensíveis não devem ser commitadas. Use `.env` local.

## Execução

- Subir serviços:
  ```bash
  astro dev start
  ```

- Parar serviços:
  ```bash
  astro dev stop
  ```

- Reiniciar (útil após trocar dependências):
  ```bash
  astro dev restart
  ```

## Serviços e Acessos

- Airflow Webserver: http://localhost:8080
- Metabase: http://localhost:3000

As credenciais padrão do Airflow podem ser definidas via variáveis ou imagem base; caso não funcionem, consulte os logs do container para confirmar a configuração.

## Configuração de Integrações

### Airflow → Snowflake

Crie a conexão no Airflow UI:
1. Acesse Admin → Connections → Add.
2. Preencha:
    - Conn Id: `snowflake_dev`
    - Conn Type: Snowflake
    - Account: sua-conta
    - Login: seu_usuario
    - Password: sua_senha
    - Warehouse: `LAB_WH_DBT`
    - Database: `LAB_PIPELINE`
    - Role: `DBT_DEV`
    - Schema padrão pode ser configurado no DAG/dbt.

Essa conexão é utilizada pela orquestração dbt via Cosmos.

### Airflow → Airbyte Cloud

Se usar o DAG de ingestão com Airbyte:
1. Crie no Airflow uma Connection para o Airbyte Cloud (ex.: `airbyte_cloud_default`) com:
    - Host: base da API (ex.: `https://api.airbyte.com`)
    - Login/Password ou API Key conforme necessário.
2. No `.env`, forneça:
    - `AIRBYTE_API_BASE_URL`
    - `AIRBYTE_CONNECTION_ID`
    - `AIRBYTE_API_KEY` (se aplicável)

O pipeline usa a API do Airbyte para atualizar a fonte e disparar sincronizações por mês do período configurado.

### Metabase

1. Acesse http://localhost:3000 e conclua o onboarding.
2. Adicione o Snowflake como fonte de dados (usar as mesmas credenciais da conexão do Airflow/Snowflake).
3. Explore as tabelas e crie dashboards.

## DAGs Disponíveis

- ingest_ons_geracao_usinas
    - Objetivo: Orquestrar ingestão mensal dos dados de geração de usinas da ONS a partir de arquivos Parquet públicos.
    - Integrações: Airbyte Cloud (API), Snowflake (destino).
    - Como usar:
        1. Ajuste as datas no `.env` (START_DATE_ONS_DATA_INGESTION / END_DATE_ONS_DATA_INGESTION).
        2. Configure as credenciais e a Connection do Airbyte Cloud no Airflow.
        3. Ative e rode o DAG manualmente no Airflow.

- laboratorio_dbt
    - Objetivo: Executar modelos dbt integrados via Cosmos, publicando no Snowflake.
    - Requisitos:
        - Conexão `snowflake_dev` configurada no Airflow.
        - Banco, schema, warehouse e role existentes no Snowflake.
    - Como usar:
        1. Configure a conexão Snowflake.
        2. Ative e rode o DAG no Airflow.
        3. Os modelos dbt definidos no projeto serão executados.

## Estrutura do Projeto
``` plaintext
local-data-stack/
├── dags/                         # DAGs do Airflow
│   ├── ingest_ons_geracao_usinas.py   # Pipeline de ingestão ONS
│   └── laboratorio_dbt.py             # Orquestração dbt
├── include/                      # Artefatos auxiliares (ex.: projetos dbt)
│   ├── dbt/                      # Projetos dbt
│   │   └── ons/                  # Projeto dbt para dados ONS
│   └── utils.py                  # Funções utilitárias
├── plugins/                      # Plugins do Airflow (se aplicável)
├── tests/                        # Testes
├── docker-compose.override.yml   # Configuração do Metabase
├── Dockerfile                    # Imagem customizada do Airflow
├── packages.txt                  # Pacotes de sistema necessários
├── requirements.txt              # Dependências Python do Airflow
├── .env                          # Variáveis de ambiente locais (não versionado)
└── README.md
```

## Fluxo de Dados

1. **Ingestão**: Airbyte Cloud extrai dados mensais de arquivos Parquet da ONS
2. **Transformação**: dbt processa e transforma os dados no Snowflake
3. **Orquestração**: Airflow coordena todo o pipeline
4. **Visualização**: Metabase conecta ao Snowflake para análises

### Executar o Pipeline de Ingestão

1. No Airflow UI, vá para a página de DAGs
2. Localize o DAG `ingest_ons_geracao_usinas`
3. Ative o toggle (liga/desliga)
4. Clique no botão Play para executar manualmente

### Executar Transformações dbt

1. No Airflow UI, localize o DAG `laboratorio_dbt`
2. Ative e execute
3. O Cosmos executará todos os modelos dbt automaticamente

### Visualizar Logs
```
bash
# Logs do Airflow
docker logs -f <container-name>-webserver-1
# Logs do Scheduler
docker logs -f <container-name>-scheduler-1
``` 

## Desenvolvimento

### Instalar dependências localmente

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

## Troubleshooting
### Airflow não inicia
``` bash
# Verificar logs
docker-compose logs

# Reiniciar serviços
astro dev restart

# Reset completo
astro dev kill
astro dev start
```
### Erro de conexão com Snowflake
- Verifique credenciais no Airflow Connections
- Confirme que o warehouse e database existem
- Teste a conexão manualmente

### Metabase não conecta ao Postgres
- Verifique o `network_mode` no `docker-compose.override.yml`
- Confirme o nome correto do container do Postgres
- Use `docker network ls` e `docker network inspect`
