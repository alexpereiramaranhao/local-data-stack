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
    ### Pipeline de Ingestão de Dados da ONS (Versão Corrigida)

    Este DAG orquestra o Airbyte Cloud para ingerir os dados mensais de geração
    de usinas do site de dados abertos da ONS.
    """

    @task
    def generate_and_update_airbyte_source_urls():
        """Gera a lista de URLs mensais e atualiza a conexão do Airbyte via API."""
        if not AIRBYTE_CONNECTION_ID_TO_SYNC:
            raise ValueError("A variável de ambiente AIRBYTE_CONNECTION_ID não está configurada!")

        start_date = datetime.strptime(START_DATE_ONS_DATA_INGESTION, "%Y/%m/%d")
        end_date = datetime.strptime(END_DATE_ONS_DATA_INGESTION, "%Y/%m/%d")
        
        base_url = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}_{month:02d}.parquet"
        urls = []
        current_date = start_date
        while current_date <= end_date:
            urls.append(base_url.format(year=current_date.year, month=current_date.month))
            current_date += relativedelta(months=1)
        
        log.info("Geradas %d URLs para sincronização.", len(urls))

        # --- Obter token OAuth2 usando Client Credentials ---
        hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)
        connection = hook.get_connection(AIRBYTE_CONN_ID)
        api_base_url = connection.host.rstrip('/')
        client_id = connection.login
        client_secret = connection.password
        
        if not client_id or not client_secret:
            raise ValueError("Client ID ou Client Secret não encontrados na conexão!")
        
        # Obter token de acesso via OAuth2
        auth_url = f"{api_base_url}/v1/applications/token"
        auth_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        
        log.info("Obtendo token de acesso do Airbyte...")
        auth_response = requests.post(auth_url, json=auth_data)
        auth_response.raise_for_status()
        
        token_data = auth_response.json()
        access_token = token_data["access_token"]
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        try:
            # Buscar informações da conexão
            get_conn_url = f"{api_base_url}/v1/connections/{AIRBYTE_CONNECTION_ID_TO_SYNC}"
            log.info("Buscando conexão: %s", get_conn_url)
            response = requests.get(get_conn_url, headers=headers)
            response.raise_for_status()
            connection_config = response.json()
            
            source_id = connection_config['sourceId']
            log.info("Source ID encontrado: %s", source_id)
            
            # Buscar configuração da fonte
            get_source_url = f"{api_base_url}/v1/sources/{source_id}"
            log.info("Buscando fonte: %s", get_source_url)
            response = requests.get(get_source_url, headers=headers)
            response.raise_for_status()
            source_config = response.json()
            
            # Verificar se a estrutura da configuração está correta
            if 'configuration' not in source_config:
                raise ValueError("Configuração da fonte não encontrada!")
                
            # Atualizar as URLs - estrutura pode variar dependendo do conector
            # Assumindo que é um conector File (CSV/Parquet)
            if 'streams' in source_config['configuration']:
                source_config['configuration']['streams'][0]['urls'] = urls
            elif 'url' in source_config['configuration']:
                # Para conectores que aceitam apenas uma URL, usar a primeira
                source_config['configuration']['url'] = urls[0] if urls else ""
            elif 'dataset_name' in source_config['configuration']:
                # Para conectores específicos, pode ser necessário ajustar
                log.warning("Estrutura de configuração não reconhecida. Tentando atualizar urls...")
                source_config['configuration']['urls'] = urls
            
            # Preparar dados para atualização
            update_source_data = {
                "configuration": source_config['configuration'],
                "name": source_config['name']
            }
            
            # Atualizar a fonte
            patch_source_url = f"{api_base_url}/v1/sources/{source_id}"
            log.info("Atualizando fonte: %s", patch_source_url)
            response = requests.patch(patch_source_url, headers=headers, json=update_source_data)
            response.raise_for_status()
            
            log.info("Fonte %s do Airbyte atualizada com sucesso.", source_id)
            return {"source_id": source_id, "urls_count": len(urls)}
            
        except requests.exceptions.RequestException as e:
            log.error("Erro na requisição HTTP: %s", str(e))
            if hasattr(e, 'response') and e.response is not None:
                log.error("Response status: %s", e.response.status_code)
                log.error("Response body: %s", e.response.text)
            raise
        except Exception as e:
            log.error("Erro inesperado: %s", str(e))
            raise

    @task
    def trigger_airbyte_sync():
        """Dispara a sincronização e aguarda a conclusão."""
        log.info("Disparando sincronização para a conexão: %s", AIRBYTE_CONNECTION_ID_TO_SYNC)
        
        # --- Obter token OAuth2 usando Client Credentials (mesmo da primeira task) ---
        hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)
        connection = hook.get_connection(AIRBYTE_CONN_ID)
        api_base_url = connection.host.rstrip('/')
        client_id = connection.login
        client_secret = connection.password
        
        if not client_id or not client_secret:
            raise ValueError("Client ID ou Client Secret não encontrados na conexão!")
        
        # Obter token de acesso via OAuth2
        auth_url = f"{api_base_url}/v1/applications/token"
        auth_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        
        log.info("Obtendo token de acesso do Airbyte...")
        auth_response = requests.post(auth_url, json=auth_data)
        auth_response.raise_for_status()
        
        token_data = auth_response.json()
        access_token = token_data["access_token"]
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        try:
            # Disparar a sincronização usando a API v1
            sync_url = f"{api_base_url}/v1/jobs"
            sync_data = {
                "connectionId": AIRBYTE_CONNECTION_ID_TO_SYNC,
                "jobType": "sync"
            }
            
            log.info("Disparando sincronização via API v1: %s", sync_url)
            response = requests.post(sync_url, headers=headers, json=sync_data)
            response.raise_for_status()
            
            job_data = response.json()
            job_id = job_data.get("jobId")
            
            if not job_id:
                log.error("Job ID não encontrado na resposta: %s", job_data)
                raise ValueError("Job ID não encontrado!")
            
            log.info("Job %s iniciado com sucesso!", job_id)
            
            # Aguardar conclusão do job
            max_wait_time = 1800  # 30 minutos
            wait_interval = 30    # 30 segundos
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                # Verificar status do job
                job_status_url = f"{api_base_url}/v1/jobs/{job_id}"
                status_response = requests.get(job_status_url, headers=headers)
                status_response.raise_for_status()
                
                job_status_data = status_response.json()
                status = job_status_data.get("status")
                
                log.info("Status do job %s: %s", job_id, status)
                
                if status in ["succeeded", "completed"]:
                    log.info("Job %s concluído com sucesso!", job_id)
                    return {"job_id": job_id, "status": status, "elapsed_time": elapsed_time}
                elif status in ["failed", "cancelled"]:
                    log.error("Job %s falhou com status: %s", job_id, status)
                    raise ValueError(f"Job falhou com status: {status}")
                elif status in ["running", "pending"]:
                    log.info("Job %s ainda em execução. Aguardando %d segundos...", job_id, wait_interval)
                    import time
                    time.sleep(wait_interval)
                    elapsed_time += wait_interval
                else:
                    log.warning("Status desconhecido: %s. Continuando...", status)
                    import time
                    time.sleep(wait_interval)
                    elapsed_time += wait_interval
            
            # Se chegou aqui, timeout
            log.warning("Timeout aguardando conclusão do job %s após %d segundos", job_id, max_wait_time)
            return {"job_id": job_id, "status": "timeout", "elapsed_time": elapsed_time}
            
        except requests.exceptions.RequestException as e:
            log.error("Erro na requisição HTTP: %s", str(e))
            if hasattr(e, 'response') and e.response is not None:
                log.error("Response status: %s", e.response.status_code)
                log.error("Response body: %s", e.response.text)
            raise
        except Exception as e:
            log.error("Erro inesperado: %s", str(e))
            raise

    # Definir dependências
    update_task = generate_and_update_airbyte_source_urls()
    sync_task = trigger_airbyte_sync()
    
    update_task >> sync_task

ingest_ons_dag()