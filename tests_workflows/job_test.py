import time
import requests
import sys
import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = "33113479481249"  # Substitua pelo ID do Job correto

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

def start_job():
    """Inicia o Job no Databricks e retorna o run_id."""
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    payload = {"job_id": JOB_ID}

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200:
        print(f"❌ Erro ao iniciar o job: {response.text}")
        sys.exit(1)

    run_id = response.json().get("run_id")
    print(f"✅ Job iniciado com Run ID: {run_id}")
    return run_id

def get_job_status(run_id):
    """Verifica o status do job até que ele termine."""
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Erro ao obter status do job: {response.text}")
            sys.exit(1)

        job_status = response.json().get("state", {}).get("life_cycle_state")
        result_state = response.json().get("state", {}).get("result_state")

        print(f"⏳ Status atual do Job: {job_status}")

        if job_status in ["TERMINATED", "SKIPPED"]:
            if result_state == "SUCCESS":
                print("✅ Job finalizado com sucesso!")
                sys.exit(0)  # Retorno 0 significa sucesso
            else:
                print(f"❌ Job falhou com status: {result_state}")
                sys.exit(1)  # Retorno 1 significa falha

        time.sleep(10)  # Aguarda 10 segundos antes de checar novamente

if __name__ == "__main__":
    run_id = start_job()
    get_job_status(run_id)
