import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Initialize SparkContext and StreamingContext with a 10-second batch interval
app_name = "exercici_10_2_toots_per_idioma"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[*]", appName = app_name)
except ValueError:
    sc.stop()
    sc = SparkContext("local[*]", appName = app_name)
sc.setLogLevel("ERROR")

# Create the StreamingContext
batch_interval = 10  # Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint_10_2")  # Necessary for updateStateByKey operation

# Define stream parameters
socket_host = "localhost"  # or IP / localhost
socket_port = 9999

kafkaStream = ssc.socketTextStream(socket_host, socket_port)

# Count the number of toots per language
# Comptem el nombre de toots originals per idioma.
tootLangCounts = (
    kafkaStream

    # Convertim cada línia rebuda pel socket en un diccionari Python.
    .map(lambda x: json.loads(x.strip()))

    # Ens quedem només amb els toots originals.
    # Si "reblog" és None, no és un retoot.
    .filter(lambda toot: toot.get("reblog") is None)

    # Eliminem els toots sense idioma informat.
    .filter(lambda toot: toot.get("language") is not None)

    # Creem parelles (idioma, 1).
    .map(lambda toot: (toot.get("language"), 1))

    # Sumem els toots de cada idioma dins del batch actual.
    .reduceByKey(lambda a, b: a + b)

    # Ordenem de més a menys segons el recompte.
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
)


# Print the cumulative count
tootLangCounts.pprint(5)

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()