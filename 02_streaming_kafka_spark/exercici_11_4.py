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

# Tipus necessaris per definir manualment l'esquema dels JSONs.
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# Funcions de Spark SQL:
# - from_json: interpreta el JSON del camp value de Kafka.
# - col: permet referenciar columnes.
# - to_timestamp: converteix text a timestamp.
# - struct: crea una columna estructurada.
# - coalesce i lit: permeten substituir valors null per 0.
from pyspark.sql.functions import from_json, col, to_timestamp, struct, coalesce, lit


# ------------------------------------------------------------
# Configuració de Spark
# ------------------------------------------------------------
conf = SparkConf()
conf.setMaster("local[*]")

sc = SparkContext(conf=conf)

# Reduïm els logs per fer la sortida més llegible.
sc.setLogLevel("ERROR")


# ------------------------------------------------------------
# Creació de la SparkSession
# ------------------------------------------------------------
# El nom de l'aplicació ha d'incloure els usuaris del grup.
app_name = "activity3_4_jbaigesf_mserrar"

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()


# ------------------------------------------------------------
# Definició manual de l'esquema d'entrada
# ------------------------------------------------------------
# En aquest exercici l'enunciat ens proporciona l'estructura dels JSONs.
# Per això no cal inferir l'esquema amb una lectura batch.
#
# Cada missatge té:
# - window: una estructura amb start i end.
# - mastodon_instance: domini o instància de Mastodon.
# - count: recompte de toots o retoots en aquella finestra.
schema = StructType([
    StructField(
        "window",
        StructType([
            StructField("start", StringType(), True),
            StructField("end", StringType(), True)
        ]),
        True
    ),
    StructField("mastodon_instance", StringType(), True),
    StructField("count", IntegerType(), True)
])


# ------------------------------------------------------------
# Paràmetres de Kafka
# ------------------------------------------------------------
# Broker Kafka del clúster.
kafka_bootstrap_servers = "eimtcld3node1:9092"

# Topic amb recomptes de toots originals per domini.
toots_original_topic = "mastodon_toots_original_domain"

# Topic amb recomptes de retoots per domini.
toots_retoot_topic = "mastodon_toots_retoot_domain"


# ------------------------------------------------------------
# Lectura del flux de toots originals
# ------------------------------------------------------------
# Llegim el primer topic en mode streaming.
#
# Utilitzem startingOffsets="latest" per evitar processar tot l'històric
# del topic i reduir el risc de problemes de memòria.
#
# maxOffsetsPerTrigger limita el nombre de missatges llegits en cada
# microbatch.
toots_original = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", toots_original_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5) \
    .load()


# ------------------------------------------------------------
# Parseig del flux de toots originals
# ------------------------------------------------------------
# Convertim el camp value de Kafka a string i l'interpretem com a JSON
# utilitzant l'esquema definit manualment.
#
# També convertim window.start i window.end a timestamp. Això ens permet
# treballar amb temps d'esdeveniment i aplicar watermark.
toots_original_df = (
    toots_original

    # Interpretem el JSON del camp value.
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("parsed_value")
    )

    # Seleccionem i preparem les columnes necessàries.
    .select(
        to_timestamp(col("parsed_value.window.start")).alias("window_start"),
        to_timestamp(col("parsed_value.window.end")).alias("window_end"),
        col("parsed_value.mastodon_instance").alias("mastodon_instance"),
        col("parsed_value.count").alias("original_count")
    )

    # Eliminem possibles registres mal formats.
    .filter(col("window_start").isNotNull())
    .filter(col("window_end").isNotNull())
    .filter(col("mastodon_instance").isNotNull())

    # Reconstruïm la columna window amb start i end, tal com demana l'enunciat.
    .select(
        struct(
            col("window_start").alias("start"),
            col("window_end").alias("end")
        ).alias("window"),
        col("window_start"),
        col("window_end"),
        col("mastodon_instance"),
        col("original_count")
    )

    # Watermark sobre el temps de finalització de la finestra.
    # Això ajuda Spark a gestionar l'estat de la unió entre streams.
    .withWatermark("window_end", "2 minutes")
)


# ------------------------------------------------------------
# Lectura del flux de retoots
# ------------------------------------------------------------
# Llegim el segon topic en mode streaming.
toots_retoot = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", toots_retoot_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5) \
    .load()


# ------------------------------------------------------------
# Parseig del flux de retoots
# ------------------------------------------------------------
# Apliquem el mateix esquema, però ara el camp count representa
# el nombre de retoots.
toots_retoot_df = (
    toots_retoot

    # Interpretem el JSON del camp value.
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("parsed_value")
    )

    # Preparem les columnes necessàries per a la unió.
    .select(
        to_timestamp(col("parsed_value.window.start")).alias("window_start"),
        to_timestamp(col("parsed_value.window.end")).alias("window_end"),
        col("parsed_value.mastodon_instance").alias("mastodon_instance"),
        col("parsed_value.count").alias("retoot_count")
    )

    # Eliminem possibles registres incomplets o mal formats.
    .filter(col("window_start").isNotNull())
    .filter(col("window_end").isNotNull())
    .filter(col("mastodon_instance").isNotNull())

    # Watermark del segon flux.
    # En una left join entre streams, el watermark és especialment important
    # per poder tancar finestres i descartar estat antic.
    .withWatermark("window_end", "2 minutes")
)


# ------------------------------------------------------------
# Left join entre els dos fluxos
# ------------------------------------------------------------
# Fem una left join perquè volem conservar tots els registres del flux
# de toots originals, encara que no hi hagi cap registre equivalent
# al flux de retoots.
#
# La unió es fa per:
# - mateixa instància de Mastodon,
# - mateix inici de finestra,
# - mateix final de finestra.
#
# També afegim una condició temporal redundant però útil per explicitar
# que la unió es fa sobre la mateixa finestra temporal.
original_alias = toots_original_df.alias("original")
retoot_alias = toots_retoot_df.alias("retoot")

toots_join_df = (
    original_alias
    .join(
        retoot_alias,
        (
            (col("original.mastodon_instance") == col("retoot.mastodon_instance")) &
            (col("original.window_start") == col("retoot.window_start")) &
            (col("original.window_end") == col("retoot.window_end"))
        ),
        "leftOuter"
    )

    # Seleccionem les columnes demanades per l'enunciat.
    .select(
        col("original.window").alias("window"),
        col("original.mastodon_instance").alias("mastodon_instance"),
        col("original.original_count").alias("original_count"),

        # Si no hi ha coincidència al flux de retoots, retoot_count serà null.
        # Amb coalesce el substituïm per 0 perquè la sortida sigui més clara.
        coalesce(
            col("retoot.retoot_count"),
            lit(0)
        ).alias("retoot_count")
    )
)


# ------------------------------------------------------------
# Sortida per consola
# ------------------------------------------------------------
# En una unió entre dos streams, el mode de sortida més adequat és append.
#
# Fem servir append perquè la consulta escriu files resultants de la unió
# quan Spark ja pot emetre-les segons l'estat i els watermarks.
#
# Complete no és adequat aquí perquè no estem mostrant una taula agregada
# global com als exercicis 11.2 i 11.3, sinó el resultat d'una unió
# entre dos fluxos.
try:
    query = (
        toots_join_df
        .writeStream

        # Mode append per emetre les files resultants de la unió.
        .outputMode("append")

        # Mostrem la sortida per consola.
        .format("console")

        # Mostrem totes les columnes sense truncar.
        .option("truncate", "false")

        # Mostrem fins a 100 files per microbatch.
        .option("numRows", 100)

        # Checkpoint propi de l'exercici.
        # La unió entre streams és una operació amb estat, per tant
        # és recomanable tenir checkpoint.
        .option("checkpointLocation", "checkpoint_11_4")

        # Actualitzem la consulta cada 10 segons.
        .trigger(processingTime="10 seconds")

        # Iniciem la consulta.
        .start()
    )

    # Mantenim la consulta activa fins que l'aturem manualment.
    query.awaitTermination()

except KeyboardInterrupt:
    # Aturem correctament la consulta i Spark si fem CTRL + C.
    query.stop()
    spark.stop()
    sc.stop()