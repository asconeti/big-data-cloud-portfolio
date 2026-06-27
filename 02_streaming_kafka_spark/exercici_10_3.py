import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Initialize SparkContext and StreamingContext with a 10-second batch interval
app_name = "exercici_10_3_recompte_acumulat_idioma" # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[*]", appName=app_name) # MODIFICAT
except ValueError:
    sc.stop()
    sc = SparkContext("local[*]", appName=app_name) # MODIFICAT

sc.setLogLevel("ERROR")

batch_interval = 10  # Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint_10_3")  # Necessary for updateStateByKey operation

# Define stream parameters
socket_host = "localhost"  # or IP / localhost
socket_port = 9999

kafkaStream = ssc.socketTextStream(socket_host, socket_port)

# Update the cumulative count using updateStateByKey
# Funció que actualitza el recompte acumulat.
def updateFunction(newValues, runningCount):
    # Si encara no hi ha recompte acumulat, comencem des de 0.
    if runningCount is None:
        runningCount = 0
    
    # Sumem els valors nous del batch al recompte anterior.
    return sum(newValues) + runningCount

# Count each toot as 1 and update the total count
# Comptem els toots originals per idioma i mantenim el recompte acumulat.
tootCounts = (
    kafkaStream

    # Convertim cada línia JSON rebuda pel socket en un diccionari Python.
    .map(lambda x: json.loads(x.strip()))

    # Ens quedem només amb els toots originals.
    .filter(lambda toot: toot.get("reblog") is None)

    # Eliminem els toots que no tenen idioma informat.
    .filter(lambda toot: toot.get("language") is not None)

    # Creem parelles (idioma, 1).
    .map(lambda toot: (toot.get("language"), 1))

    # Actualitzem el recompte acumulat per idioma entre batches.
    .updateStateByKey(updateFunction)

    # Ordenem de més a menys segons el recompte acumulat.
    .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
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