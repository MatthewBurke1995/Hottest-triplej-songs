from google.cloud import bigquery, storage


# Construct a BigQuery client object.
client = bigquery.Client()


# The name for the new bucket
bucket_name = "triplejsongs"

from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT * FROM `triplej-398802.triplejsongs.hotness_score` '
    'ORDER BY hotness_score DESC '
    'LIMIT 10')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish


with open('README_template.md', 'r') as file:
    template = file.read()


with open('README.md', 'w') as f:
    f.seek(0)
    f.write(template)
    f.truncate()
    for row in rows:
        f.write(str(row) + "\n")



