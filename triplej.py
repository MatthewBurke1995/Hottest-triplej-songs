import abc_radio_wrapper
from datetime import datetime, timedelta, timezone
import pytz
ABC = abc_radio_wrapper.ABCRadio()

endDate: datetime =  datetime.now(pytz.timezone("Australia/Sydney"))
startDate: datetime =  endDate - timedelta(days=1)


last_date = endDate
data = []
    
#search through 24hour period of triplej songs
def add_to_data(startDate, endDate):
    last_date = endDate
    print(startDate, endDate)
    search_results = 100
    for search_result in ABC.continuous_search(from_=startDate, to=endDate, station="triplej", limit=100):
        search_results = search_result.total
        for radio_play in search_result.radio_songs:
            for artist in radio_play.song.artists:
                artist_str = artist.name.replace(",","")
                song = radio_play.song.title.replace(",","")
                if radio_play.played_time < last_date:
                    last_date = radio_play.played_time

                data.append([radio_play.played_time.strftime("%Y-%m-%d %H:%M:%S"),song,artist_str])
    return last_date, search_results


last_date, _ = add_to_data(startDate,endDate)
while True:
    last_date, results = add_to_data(startDate,last_date)
    if results < 100:
        break #stop processing when we reach the last page of results



from google.cloud import bigquery, storage


# Construct a BigQuery client object.
client = bigquery.Client()

storage_client = storage.Client()


# The name for the new bucket
bucket_name = "triplejsongs"



def write_to_storage(bucket_name, blob_name, data):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("w") as f:
        for line in data:
            f.write(",".join(line) + "\n")



write_to_storage(bucket_name, "triplejsongs.csv", data )


table_id = "triplej-398802.triplejsongs.songs"

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    skip_leading_rows=0, #dont skip

    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

uri = "gs://triplejsongs/triplejsongs.csv"


load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.


load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))


