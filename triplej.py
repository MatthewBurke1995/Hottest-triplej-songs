import abc_radio_wrapper
from datetime import datetime

ABC = abc_radio_wrapper.ABCRadio()

startDate: datetime = datetime.fromisoformat("2023-01-01T00:00:00+10:00")
endDate: datetime = datetime.fromisoformat("2023-08-17T00:00:00+10:00")

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
print(last_date)
while True:
    last_date, results = add_to_data(startDate,last_date)
    if results < 100:
        break



from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()



table_id = "triplej-398802.triplejsongs.songs"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"



import csv
with open('records.tsv', 'w', newline='') as tsvfile:
    writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
    for record in data:
        writer.writerow(record)
            
