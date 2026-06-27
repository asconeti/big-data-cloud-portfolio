# ------------------------------------------------------------
# Inicialització de findspark
# ------------------------------------------------------------
# findspark permet que Python localitzi correctament la instal·lació
# de Spark dins de l'entorn del clúster.
import findspark
findspark.init()


# ------------------------------------------------------------
# Importació de llibreries
# ------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

# from_json permet interpretar el JSON del camp value de Kafka.
# col permet seleccionar columnes.
from pyspark.sql.functions import from_json, col


# ------------------------------------------------------------
# Configuració de Spark
# ------------------------------------------------------------
conf = SparkConf()
conf.setMaster("local[*]")

sc = SparkContext(conf=conf)

# Reduïm el nivell de logs per facilitar la lectura de la sortida.
sc.setLogLevel("ERROR")


# ------------------------------------------------------------
# Creació de la SparkSession
# ------------------------------------------------------------
# El nom de l'aplicació ha d'incloure els usuaris del grup.
app_name = "activity3_2_jbaigesf_mserrar"

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()


# ------------------------------------------------------------
# Paràmetres de Kafka
# ------------------------------------------------------------
# Broker Kafka del clúster de la pràctica.
kafka_bootstrap_servers = "eimtcld3node1:9092"

# Topic que conté els JSONs dels toots de Mastodon.
kafka_topic = "mastodon_toots"


# ------------------------------------------------------------
# Lectura batch petita per inferir l'esquema
# ------------------------------------------------------------
# Structured Streaming necessita conèixer l'esquema abans de processar
# el flux. Per això llegim uns quants missatges en mode batch.
#
# Important:
# A la plantilla apareixia endingOffsets amb l'offset absolut 10.
# Al nostre entorn això provocava error perquè els offsets antics ja
# no estaven disponibles. Per això fem servir endingOffsets="latest"
# i després limitem la mostra amb limit(10).
batch_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load() \
    .limit(10)


# ------------------------------------------------------------
# Inferència de l'esquema JSON
# ------------------------------------------------------------
# Kafka desa el missatge dins la columna value en format binari.
# Primer convertim value a string i després Spark infereix l'esquema JSON.
schema = spark.read.json(
    batch_df
    .selectExpr("CAST(value AS STRING)")
    .rdd
    .map(lambda x: x[0])
).schema

# Mostrem l'esquema per comprovar que s'ha inferit correctament.
print(schema.simpleString())


# ------------------------------------------------------------
# Lectura streaming des de Kafka
# ------------------------------------------------------------
# Llegim el topic de Kafka en mode streaming.
#
# Fem servir startingOffsets="latest" per evitar carregar tot l'històric
# del topic, que és molt gran i pot provocar problemes de memòria.
#
# maxOffsetsPerTrigger limita el nombre de missatges processats en cada
# microbatch. Això ajuda a mantenir controlat el consum de memòria.
toots = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5) \
    .load()


# ------------------------------------------------------------
# Parseig, filtratge i agregació
# ------------------------------------------------------------
# Objectiu de l'exercici:
# Comptar el nombre de toots originals per idioma i continuar
# acumulant aquests recomptes cada 10 segons.
#
# Passos:
# 1. Convertim value a string.
# 2. Apliquem from_json() amb l'esquema inferit.
# 3. Filtrem només els toots originals.
# 4. Ens quedem només amb els toots que tenen idioma informat.
# 5. Seleccionem la columna language.
# 6. Agrupem per language.
# 7. Comptem quants toots hi ha per idioma.
# 8. Ordenem de manera descendent perquè els idiomes amb més toots
#    apareguin a la part superior.
toots_df = (
    toots

    # Interpretem el JSON del camp value.
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("parsed_value")
    )

    # Ens quedem només amb toots originals.
    # Si reblog és null, no és un retoot.
    .filter(col("parsed_value.reblog").isNull())

    # Evitem idiomes nuls perquè volem agrupar per idioma real.
    .filter(col("parsed_value.language").isNotNull())

    # Seleccionem només la columna necessària per a l'agregació.
    .select(
        col("parsed_value.language").alias("language")
    )

    # Agrupem per idioma.
    .groupBy("language")

    # Comptem el nombre acumulat de toots originals per idioma.
    .count()

    # Ordenem de més a menys toots.
    .orderBy(col("count").desc())
)


# ------------------------------------------------------------
# Sortida per consola
# ------------------------------------------------------------
# En aquest exercici fem una agregació acumulada.
#
# Per això utilitzem outputMode("complete"):
# - append no és adequat perquè les files agregades es van actualitzant.
# - update podria mostrar només files modificades, però nosaltres volem
#   veure la taula completa i ordenada cada 10 segons.
# - complete mostra tota la taula de resultats a cada microbatch.
#
# Com que aquesta consulta manté estat acumulat, aquí sí que és recomanable
# definir un checkpoint nou i net.
try:
    query = (
        toots_df
        .writeStream

        # Mostrem tota la taula agregada a cada trigger.
        .outputMode("complete")

        # Mostrem la sortida per consola.
        .format("console")

        # No cal truncar gaire perquè només tenim language i count.
        .option("truncate", "false")

        # Mostrem com a màxim 20 idiomes.
        .option("numRows", 20)

        # Checkpoint propi d'aquest exercici.
        # És important perquè la consulta manté estat acumulat.
        .option("checkpointLocation", "checkpoint_11_2")

        # Actualitzem la sortida cada 10 segons, tal com demana l'enunciat.
        .trigger(processingTime="10 seconds")

        # Iniciem la consulta streaming.
        .start()
    )

    # Mantenim la consulta activa fins que l'aturem manualment.
    query.awaitTermination()

except KeyboardInterrupt:
    # Aturada ordenada si fem CTRL + C.
    query.stop()
    spark.stop()
    sc.stop()