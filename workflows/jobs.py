import os
import dotenv
import requests

dotenv.load_dotenv(".env")

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")

print(DATABRICKS_TOKEN)

url = f"{DATABRICKS_HOST}/api/2.2/jobs/list"	
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

resp = requests.get(url, headers=headers)
print(resp.json())
jobs = resp.json()["jobs"]
print(jobs)
