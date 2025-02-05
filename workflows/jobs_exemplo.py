import os
import dotenv
import requests
import json

dotenv.load_dotenv(".env")

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")

print(DATABRICKS_TOKEN)

url = f"{DATABRICKS_HOST}/api/2.2/jobs/list"	
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

jobs = requests.get(url, headers=headers).json()
job_id = jobs["jobs"][0]["job_id"]
print(job_id)

with(open("get_bitcoin.json", "r")) as openfile:
    settings = json.load(openfile)



def run_job(settings):
    url = f"{DATABRICKS_HOST}/api/2.2/jobs/run-now"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.post(url, headers=headers, json=settings)
    return resp

resp = run_job(settings=settings)
print(resp)