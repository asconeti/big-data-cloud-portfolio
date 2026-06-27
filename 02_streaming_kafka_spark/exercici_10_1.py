import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Initialize SparkContext and StreamingContext with a 10-second batch interval
app_name = "exercici_10_1_counting_windows"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[*]", appName = app_name)
except ValueError:
    sc.stop()
    sc = SparkContext("local[*]", appName = app_name)
sc.setLogLevel("ERROR")

# Create the StreamingContext
batch_interval = 10  # (MODIFICAT) Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint_10_1")  # Necessary for updateStateByKey operation

# Define stream parameters (Forum: El flux de Mastodon ja està disponible al clúster pel port 9999.)
socket_host = "localhost"  # or IP / localhost
socket_port = 9999

# Llegim les dades del sòcol.
kafkaStream = ssc.socketTextStream(socket_host, socket_port)

# Count each toot as 1 and update the total count
tootCounts = (
    kafkaStream
        
    # Convertim cada línia rebuda pel socket, que arriba com a text JSON,
    # en un diccionari Python.
    .map(lambda x: json.loads(x.strip())) 
        
    # Ens quedem només amb els toots originals.
    # En Mastodon, si "reblog" és None, vol dir que no és un retoot.
    .filter(lambda toot: toot.get("reblog") is None) 
        
    # Assignem la mateixa clau "count" a tots els toots originals.
    # Cada toot original compta com 1.
    .map(lambda toot: ("count", 1)) 
        
    # Sumem tots els valors amb la mateixa clau dins del batch actual.
    # Per exemple: ("count", 1), ("count", 1) -> ("count", 2).
    .reduceByKey(lambda a, b: a + b)
)

# Print the cumulative count
tootCounts.pprint()

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()