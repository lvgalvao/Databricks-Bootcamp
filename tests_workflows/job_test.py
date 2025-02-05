import os
import dotenv
import requests
import json

dotenv.load_dotenv(".env")

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")

def load_settings():
    """Carrega as configurações do job a partir de um arquivo JSON."""
    with open(f"test_worflow.json", "r") as openfile:
        settings = json.load(openfile)

    # Extraindo apenas o "job_id" e "settings" para o formato correto
    return {
        "job_id": settings["job_id"],
        "new_settings": settings["settings"]  # Garante que o reset use apenas as configurações
    }

def run_job(settings):
    url = f"{DATABRICKS_HOST}/api/2.2/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(url, headers=headers, json=settings)
    return resp

def main():
    settings = load_settings(i)
    resp = run_job(settings=settings)
    if resp.status_code == 200:
        print(f"Job {i} atualizado com sucesso.")
    else:
        print(f"Erro ao atualizar o job {i}.")

if __name__ == "__main__":
    main()