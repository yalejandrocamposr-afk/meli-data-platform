from google.cloud import bigquery

client = bigquery.Client(project="meli-data-platform")

def run_query(query: str):
    query_job = client.query(query)
    results = query_job.result()

    return [dict(row) for row in results]