# dags/ingest_ons_geracao_usinas.py

import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import logging

from airflow.decorators import dag, task
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook

# --- CONFIGURAÇÃO ---
AIRBYTE_CONN_ID = "airbyte_cloud_default"  # Nome da conexão definida no .env
AIRBYTE_CONNECTION_ID_TO_SYNC = os.getenv("AIRBYTE_CONNECTION_ID")

START_DATE_ONS_DATA_INGESTION = os.getenv("START_DATE_ONS_DATA_INGESTION", "2025/01/01")
END_DATE_ONS_DATA_INGESTION = os.getenv("END_DATE_ONS_DATA_INGESTION", "2025/12/31")

log = logging.getLogger(__name__)

@dag(
    dag_id="ingest_ons_geracao_usinas",
    start_date=datetime.strptime(START_DATE_ONS_DATA_INGESTION, "%Y/%m/%d"),
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "ons", "airbyte"],
)
def ingest_ons_dag():
    """
    ### Pipeline de Ingestão de Dados da ONS (Versão Final)

    Este DAG orquestra o Airbyte Cloud para ingerir os dados mensais de geração
    de usinas do site de dados abertos da ONS para todo o ano de 2025.

    Estratégia:
    - Gera URLs para todos os meses do período configurado
    - Atualiza a source do Airbyte com cada URL individualmente
    - Dispara e monitora a sincronização de cada arquivo
    - Fornece resumo final com sucessos e falhas
    """

    @task
    def generate_and_update_airbyte_source_urls():
        """Gera a lista de URLs mensais e atualiza a conexão do Airbyte via API."""
        if not AIRBYTE_CONNECTION_ID_TO_SYNC:
            raise ValueError("A variável de ambiente AIRBYTE_CONNECTION_ID não está configurada!")

        # --- Função para obter token fresco ---
        def get_fresh_token():
            """Obtém um token OAuth2 fresco"""
            hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)
            connection = hook.get_connection(AIRBYTE_CONN_ID)
            api_base_url = connection.host.rstrip('/')
            client_id = connection.login
            client_secret = connection.password

            if not client_id or not client_secret:
                raise ValueError("Client ID ou Client Secret não encontrados na conexão!")

            auth_url = f"{api_base_url}/v1/applications/token"
            auth_data = {
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials"
            }

            log.info("Obtendo token de acesso fresco do Airbyte...")
            auth_response = requests.post(auth_url, json=auth_data)
            auth_response.raise_for_status()

            token_data = auth_response.json()
            access_token = token_data["access_token"]

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            return api_base_url, headers

        # Obter configurações iniciais
        api_base_url, headers = get_fresh_token()

        try:
            # Buscar informações da conexão
            get_conn_url = f"{api_base_url}/v1/connections/{AIRBYTE_CONNECTION_ID_TO_SYNC}"
            log.info("Buscando conexão: %s", get_conn_url)
            response = requests.get(get_conn_url, headers=headers)
            response.raise_for_status()
            connection_config = response.json()

            source_id = connection_config['sourceId']
            log.info("Source ID encontrado: %s", source_id)

            # VERIFICAR E CORRIGIR SYNC MODE PRIMEIRO
            log.info("=== VERIFICANDO SYNC MODE ===")
            sync_mode_fixed = False

            if 'syncCatalog' in connection_config:
                sync_catalog = connection_config['syncCatalog']

                if 'streams' in sync_catalog:
                    for stream in sync_catalog['streams']:
                        current_sync_mode = stream.get('config', {}).get('syncMode')
                        log.info("Sync mode atual: %s", current_sync_mode)

                        if current_sync_mode == 'full_refresh_overwrite':
                            log.info("Corrigindo sync mode de 'overwrite' para 'append'...")

                            if 'config' not in stream:
                                stream['config'] = {}
                            stream['config']['syncMode'] = 'full_refresh_append'
                            sync_mode_fixed = True

                            log.info("Sync mode atualizado para: %s", stream['config']['syncMode'])

            # Se precisou corrigir o sync mode, atualizar a conexão
            if sync_mode_fixed:
                update_connection_data = {
                    "name": connection_config['name'],
                    "sourceId": connection_config['sourceId'],
                    "destinationId": connection_config['destinationId'],
                    "syncCatalog": connection_config['syncCatalog']
                }

                # Incluir campos obrigatórios se existirem
                for field in ['status', 'scheduleType', 'namespaceDefinition', 'namespaceFormat', 'prefix']:
                    if field in connection_config:
                        update_connection_data[field] = connection_config[field]

                patch_conn_url = f"{api_base_url}/v1/connections/{AIRBYTE_CONNECTION_ID_TO_SYNC}"
                log.info("Atualizando configuração da conexão...")
                response = requests.patch(patch_conn_url, headers=headers, json=update_connection_data)
                response.raise_for_status()
                log.info("✅ Sync mode corrigido para 'append'!")
            else:
                log.info("Sync mode já está correto")

            # Buscar configuração da fonte
            get_source_url = f"{api_base_url}/v1/sources/{source_id}"
            log.info("Buscando fonte: %s", get_source_url)
            response = requests.get(get_source_url, headers=headers)
            response.raise_for_status()
            source_config = response.json()

            log.info("Configuração atual da fonte:")
            log.info("Nome: %s", source_config.get('name'))

            # Verificar se a estrutura da configuração está correta
            if 'configuration' not in source_config:
                raise ValueError("Configuração da fonte não encontrada!")

            current_config = source_config['configuration']

            # GERAR URLs E PROCESSAR
            start_date = datetime.strptime(START_DATE_ONS_DATA_INGESTION, "%Y/%m/%d")
            end_date = datetime.strptime(END_DATE_ONS_DATA_INGESTION, "%Y/%m/%d")

            base_url = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}_{month:02d}.parquet"
            urls = []
            current_date = start_date
            while current_date <= end_date:
                urls.append(base_url.format(year=current_date.year, month=current_date.month))
                current_date += relativedelta(months=1)

            log.info("Geradas %d URLs para sincronização:", len(urls))
            for i, url in enumerate(urls):
                log.info("URL %d: %s", i+1, url)

            # ESTRATÉGIA: Como o conector File suporta apenas uma URL por vez,
            # vamos fazer múltiplas atualizações, uma para cada mês
            updated_count = 0

            for i, url in enumerate(urls):
                log.info("=== Processando URL %d/%d ===", i+1, len(urls))
                log.info("URL: %s", url)

                # Obter token fresco para cada processamento
                api_base_url, headers = get_fresh_token()

                # Atualizar a configuração com a URL atual
                updated_config = current_config.copy()
                updated_config['url'] = url

                # Preparar dados para atualização
                update_source_data = {
                    "configuration": updated_config,
                    "name": source_config['name']
                }

                # Atualizar a fonte
                patch_source_url = f"{api_base_url}/v1/sources/{source_id}"
                log.info("Atualizando fonte com nova URL: %s", patch_source_url)
                response = requests.patch(patch_source_url, headers=headers, json=update_source_data)
                response.raise_for_status()

                log.info("✅ Fonte atualizada com URL %d", i+1)

                # Disparar sincronização para esta URL
                sync_url = f"{api_base_url}/v1/jobs"
                sync_data = {
                    "connectionId": AIRBYTE_CONNECTION_ID_TO_SYNC,
                    "jobType": "sync"
                }

                log.info("Disparando sincronização para URL %d...", i+1)
                sync_response = requests.post(sync_url, headers=headers, json=sync_data)
                sync_response.raise_for_status()

                job_data = sync_response.json()
                job_id = job_data.get("jobId")

                if job_id:
                    log.info("✅ Sincronização %d iniciada: Job ID %s", i+1, job_id)

                    # Aguardar conclusão desta sincronização antes de continuar
                    max_wait_time = 600  # 10 minutos por arquivo
                    wait_interval = 15   # 15 segundos
                    elapsed_time = 0

                    while elapsed_time < max_wait_time:
                        # Obter token fresco para verificação de status se necessário
                        if elapsed_time > 0 and elapsed_time % 300 == 0:  # A cada 5 minutos
                            log.info("Renovando token para verificação de status...")
                            api_base_url, headers = get_fresh_token()

                        job_status_url = f"{api_base_url}/v1/jobs/{job_id}"
                        try:
                            status_response = requests.get(job_status_url, headers=headers)
                            status_response.raise_for_status()

                            job_status_data = status_response.json()
                            status = job_status_data.get("status")

                            log.info("Status da sincronização %d: %s", i+1, status)

                            if status in ["succeeded", "completed"]:
                                log.info("✅ Sincronização %d concluída com sucesso!", i+1)
                                updated_count += 1
                                break
                            elif status in ["failed", "cancelled"]:
                                log.error("❌ Sincronização %d falhou: %s", i+1, status)
                                # Continuar com próxima URL mesmo se uma falhar
                                break
                            elif status in ["running", "pending"]:
                                import time
                                time.sleep(wait_interval)
                                elapsed_time += wait_interval
                            else:
                                log.warning("Status desconhecido para sincronização %d: %s", i+1, status)
                                import time
                                time.sleep(wait_interval)
                                elapsed_time += wait_interval

                        except requests.exceptions.RequestException as status_error:
                            log.warning("Erro ao verificar status (tentando renovar token): %s", str(status_error))
                            # Tentar renovar token e continuar
                            try:
                                api_base_url, headers = get_fresh_token()
                                import time
                                time.sleep(wait_interval)
                                elapsed_time += wait_interval
                            except Exception as token_error:
                                log.error("Falha ao renovar token: %s", str(token_error))
                                raise

                    if elapsed_time >= max_wait_time:
                        log.warning("⏰ Timeout na sincronização %d após %d segundos", i+1, max_wait_time)
                else:
                    log.error("❌ Falha ao obter Job ID para sincronização %d", i+1)

                # Pequena pausa entre processamentos
                if i < len(urls) - 1:  # Não pausar no último
                    import time
                    time.sleep(5)

            log.info("=== RESUMO FINAL ===")
            log.info("Total de URLs processadas: %d", len(urls))
            log.info("Sincronizações bem-sucedidas: %d", updated_count)
            log.info("Sincronizações com falha: %d", len(urls) - updated_count)

            return {
                "source_id": source_id,
                "total_urls": len(urls),
                "successful_syncs": updated_count,
                "failed_syncs": len(urls) - updated_count,
                "sync_mode_fixed": sync_mode_fixed
            }

        except requests.exceptions.RequestException as e:
            log.error("Erro na requisição HTTP: %s", str(e))
            if hasattr(e, 'response') and e.response is not None:
                log.error("Response status: %s", e.response.status_code)
                log.error("Response body: %s", e.response.text)
            raise
        except Exception as e:
            log.error("Erro inesperado: %s", str(e))
            raise

    # Definir dependências - agora só precisa de uma task
    sync_all_task = generate_and_update_airbyte_source_urls()

ingest_ons_dag()