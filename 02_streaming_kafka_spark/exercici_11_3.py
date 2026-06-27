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
# window permet crear finestres temporals sobre una columna timestamp.
# to_timestamp converteix el camp created_at, que ve com a text, a timestamp.
from pyspark.sql.functions import from_json, col, window, to_timestamp


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
app_name = "activity3_3_jbaigesf_mserrar"

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
# Structured Streaming necessita conèixer l'esquema JSON abans
# de poder aplicar from_json() al flux.
#
# Fem una lectura batch petita del topic per inferir l'esquema.
# Utilitzem endingOffsets="latest" perquè l'offset absolut 10 de la
# plantilla provocava errors al nostre entorn.
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
# Primer convertim value a string i després Spark infereix l'esquema.
schema = spark.read.json(
    batch_df
    .selectExpr("CAST(value AS STRING)")
    .rdd
    .map(lambda x: x[0])
).schema

# Mostrem l'esquema inferit per comprovar que conté camps com:
# created_at, language, reblog, account, etc.
print(schema.simpleString())


# ------------------------------------------------------------
# Lectura streaming des de Kafka
# ------------------------------------------------------------
# Llegim el topic en mode streaming.
#
# startingOffsets="latest" evita processar tot l'històric del topic.
# maxOffsetsPerTrigger limita el nombre de missatges processats
# en cada microbatch i ajuda a evitar problemes de memòria.
toots = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5) \
    .load()


# ------------------------------------------------------------
# Parseig, filtratge i agregació amb finestra temporal
# ------------------------------------------------------------
# Objectiu:
# Comptar el nombre de toots originals per idioma dins de finestres
# temporals d'1 minut, actualitzades cada 5 segons.
#
# Estructura de sortida demanada:
# - window: rang temporal de la finestra.
# - language: idioma del toot.
# - count: nombre de toots originals d'aquell idioma dins la finestra.
toots_df = (
    toots

    # Convertim el camp value de Kafka a string i l'interpretem com a JSON.
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("parsed_value")
    )

    # Filtrem només els toots originals.
    # Si reblog és null, el missatge és un toot original.
    # Si reblog conté una estructura, el missatge és un retoot.
    .filter(col("parsed_value.reblog").isNull())

    # Evitem registres sense idioma, perquè l'enunciat demana
    # fer el recompte per idioma.
    .filter(col("parsed_value.language").isNotNull())

    # Seleccionem les columnes necessàries.
    # Convertim created_at a timestamp perquè window() necessita
    # una columna temporal real, no una cadena de text.
    .select(
        to_timestamp(col("parsed_value.created_at")).alias("created_at"),
        col("parsed_value.language").alias("language")
    )

    # Eliminem possibles registres on created_at no s'hagi pogut convertir.
    .filter(col("created_at").isNotNull())

    # Agrupem per finestra temporal i idioma.
    # La finestra dura 1 minut i es recalcula cada 5 segons.
    .groupBy(
        window(col("created_at"), "1 minute", "5 seconds"),
        col("language")
    )

    # Comptem els toots originals de cada idioma dins de cada finestra.
    .count()

    # Ordenem els resultats:
    # 1. Primer per inici de finestra en ordre descendent.
    # 2. Després pel recompte en ordre descendent.
    .orderBy(
        col("window.start").desc(),
        col("count").desc()
    )
)


# ------------------------------------------------------------
# Sortida per consola
# ------------------------------------------------------------
# Aquesta consulta fa una agregació amb finestres temporals.
#
# Utilitzem outputMode("complete") perquè volem veure tota la taula
# agregada en cada microbatch.
#
# L'actualització es fa cada 5 segons, tal com demana l'enunciat.
try:
    query = (
        toots_df
        .writeStream

        # Complete mostra tota la taula agregada a cada microbatch.
        .outputMode("complete")

        # Mostrem els resultats per consola.
        .format("console")

        # No trunquem perquè només mostrem window, language i count.
        .option("truncate", "false")

        # Mostrem un màxim de 30 files per microbatch.
        .option("numRows", 30)

        # Checkpoint propi de l'exercici 11.3.
        # Com que hi ha agregació amb finestra, és recomanable mantenir-lo.
        .option("checkpointLocation", "checkpoint_11_3")

        # Refresquem els resultats cada 5 segons.
        .trigger(processingTime="5 seconds")

        # Iniciem la consulta.
        .start()
    )

    # Mantenim la consulta activa fins que l'aturem manualment.
    query.awaitTermination()

except KeyboardInterrupt:
    # Aturada ordenada si fem CTRL + C.
    query.stop()
    spark.stop()
    sc.stop()