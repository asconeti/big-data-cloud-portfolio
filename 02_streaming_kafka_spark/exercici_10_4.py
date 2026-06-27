import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "exercici_10_4_windowed_counting"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[*]", appName=app_name) # MODIFICAT
except ValueError:
    sc.stop()
    sc = SparkContext("local[*]", appName=app_name) # MODIFICAT

sc.setLogLevel("ERROR")

# Fem batches de 5 segons perquè la finestra s'actualitza cada 5 segons.
batch_interval = 5
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint_10_4")  # Necessary for updateStateByKey operation

# Define stream parameters
socket_host = "localhost"  # or IP / localhost
socket_port = 9999

kafkaStream = ssc.socketTextStream(socket_host, socket_port)

# Count each toot as 1 and update the total count. Use a 60-second window with a 5-second slide
# Comptem els toots originals per idioma dins d'una finestra temporal.
tootCounts = (
    kafkaStream

    # Convertim cada línia JSON rebuda pel socket en un diccionari Python.
    .map(lambda x: json.loads(x.strip()))

    # Ens quedem només amb els toots originals.
    .filter(lambda toot: toot.get("reblog") is None)

    # Eliminem els toots sense idioma informat.
    .filter(lambda toot: toot.get("language") is not None)

    # Creem parelles (idioma, 1).
    .map(lambda toot: (toot.get("language"), 1))

    # Comptem per idioma dins dels darrers 60 segons,
    # actualitzant el resultat cada 5 segons.
    .reduceByKeyAndWindow(
        lambda a, b: a + b,   # Sumem els nous valors que entren a la finestra.
        lambda a, b: a - b,   # Restem els valors antics que surten de la finestra.
        60,                   # Durada de la finestra: 60 segons.
        5                     # Actualització cada 5 segons.
    )
     
    # Ordenem de més a menys segons el recompte de la finestra.
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
)

# Print the cumulative count
# Mostrem només els 10 idiomes principals.
tootCounts.pprint(10)

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()