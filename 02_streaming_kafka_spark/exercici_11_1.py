# ------------------------------------------------------------
# Inicialització de findspark
# ------------------------------------------------------------
# findspark permet que Python localitzi correctament la instal·lació
# de Spark dins de l'entorn del clúster.
# És habitual utilitzar-lo en entorns Jupyter o en entorns on Spark
# no està directament configurat al PYTHONPATH.
import findspark
findspark.init()


# ------------------------------------------------------------
# Importació de llibreries de PySpark
# ------------------------------------------------------------
# SparkSession és el punt d'entrada principal per treballar amb
# DataFrames i Structured Streaming.
from pyspark.sql import SparkSession

# SparkConf i SparkContext permeten configurar i inicialitzar Spark.
from pyspark import SparkConf, SparkContext

# from_json permet convertir una cadena JSON en una estructura Spark.
# col permet referenciar columnes del DataFrame.
from pyspark.sql.functions import from_json, col


# ------------------------------------------------------------
# Configuració bàsica de Spark
# ------------------------------------------------------------
# Creem una configuració de Spark.
conf = SparkConf()

# Treballem en mode local utilitzant tots els nuclis disponibles.
# local[*] indica que Spark pot utilitzar tots els cores de la màquina.
conf.setMaster("local[*]")

# Creem el SparkContext, que és el context base de Spark.
# En aquest script només en creem un.
sc = SparkContext(conf=conf)

# Reduïm el nivell de logs per evitar que la sortida sigui massa extensa.
# Això fa que no apareguin tants missatges INFO durant l'execució.
sc.setLogLevel("ERROR")


# ------------------------------------------------------------
# Creació de la SparkSession
# ------------------------------------------------------------
# SparkSession és necessària per treballar amb DataFrames,
# llegir dades de Kafka amb readStream i aplicar Structured Streaming.
#
# L'enunciat indica que el nom de l'aplicació ha d'incloure
# els usuaris dels membres del grup.
app_name = "activity3_1_jbaigesf_mserrar"

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()


# ------------------------------------------------------------
# Paràmetres de connexió a Kafka
# ------------------------------------------------------------
# Aquest és el servidor Kafka del clúster de la pràctica.
kafka_bootstrap_servers = "eimtcld3node1:9092"

# Topic que conté els missatges JSON dels toots de Mastodon.
# Aquest topic l'hem comprovat prèviament amb kafka-console-consumer.sh.
kafka_topic = "mastodon_toots"


# ------------------------------------------------------------
# Lectura batch petita per inferir l'esquema JSON
# ------------------------------------------------------------
# Structured Streaming necessita conèixer l'esquema de les dades
# abans de poder aplicar from_json() sobre el flux.
#
# Com que els missatges de Kafka són JSONs dins de la columna value,
# fem primer una lectura batch petita del topic per inferir-ne l'esquema.
#
# Important:
# - startingOffsets="earliest" indica que, per a aquesta lectura batch,
#   comencem pel primer offset disponible actualment.
# - endingOffsets="latest" indica que llegim fins a l'últim offset disponible.
# - limit(10) evita processar massa missatges només per inferir l'esquema.
#
# Aquesta lectura només serveix per obtenir l'esquema. No és la lectura
# streaming principal de l'exercici.
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
# Kafka desa el contingut del missatge a la columna value.
# Aquesta columna és de tipus binari, per això primer la convertim a string.
#
# La línia següent és la de la plantilla:
# 1. Selecciona value convertit a string.
# 2. Converteix el DataFrame a RDD.
# 3. Extreu el text JSON de cada fila.
# 4. Fa que Spark infereixi automàticament l'esquema JSON.
schema = spark.read.json(
    batch_df
    .selectExpr("CAST(value AS STRING)")
    .rdd
    .map(lambda x: x[0])
).schema

# Mostrem l'esquema inferit per pantalla.
# Això ens permet comprovar que Spark ha detectat camps com:
# account, content, created_at, language, reblog, etc.
print(schema.simpleString())


# ------------------------------------------------------------
# Lectura streaming des de Kafka
# ------------------------------------------------------------
# Ara sí que definim la lectura en streaming.
# Aquesta lectura queda activa i va processant nous missatges del topic.
#
# Fem servir startingOffsets="latest" perquè el topic conté molts missatges
# històrics i llegir-los tots podia provocar problemes de memòria.
#
# maxOffsetsPerTrigger limita el nombre màxim de missatges processats
# en cada microbatch. Això ajuda a controlar la càrrega de treball i evita
# que Spark intenti processar massa dades de cop.
toots = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5) \
    .load()


# ------------------------------------------------------------
# Parseig del JSON i selecció de columnes
# ------------------------------------------------------------
# El DataFrame streaming original de Kafka conté columnes com:
# key, value, topic, partition, offset i timestamp.
#
# La informació real del toot és dins de value.
# Per això:
# 1. Convertim value a string.
# 2. Apliquem from_json() utilitzant l'esquema inferit.
# 3. Guardem el resultat en una columna estructurada anomenada parsed_value.
# 4. Filtrem només els toots originals.
# 5. Seleccionem les columnes que demana l'enunciat.
toots_df = (
    toots

    # Convertim el camp value de Kafka a string i el parsegem com a JSON.
    # El resultat és una estructura Spark amb els camps del toot.
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("parsed_value")
    )

    # Ens quedem només amb els toots originals.
    # En Mastodon, el camp reblog indica si el toot és un retoot.
    # - Si reblog és null, el missatge és un toot original.
    # - Si reblog conté una estructura, el missatge és un retoot.
    .filter(col("parsed_value.reblog").isNull())

    # Seleccionem les columnes demanades per l'enunciat.
    # account.username conté el nom d'usuari.
    # account.followers_count conté el nombre de seguidors de l'usuari.
    .select(
        col("parsed_value.id").alias("id"),
        col("parsed_value.created_at").alias("created_at"),
        col("parsed_value.content").alias("content"),
        col("parsed_value.language").alias("language"),
        col("parsed_value.account.username").alias("username"),
        col("parsed_value.account.followers_count").alias("followers_count")
    )
)


# ------------------------------------------------------------
# Sortida del flux per consola
# ------------------------------------------------------------
# Com que aquesta consulta no fa cap agregació, ni manté estat,
# ni actualitza files anteriors, el mode de sortida adequat és append.
#
# append vol dir que Spark només mostra les noves files que arriben
# en cada microbatch.
#
# En aquest exercici no utilitzem checkpointLocation perquè la consulta
# és simple: només parseja, filtra i selecciona columnes.

try:
    query = (
        toots_df
        .writeStream

        # Mode append: només s'escriuen les noves files processades.
        .outputMode("append")

        # Mostrem els resultats per consola.
        .format("console")

        # truncate=true evita imprimir continguts HTML molt llargs sencers.
        # El camp content pot contenir etiquetes HTML i textos llargs.
        .option("truncate", "true")

        # Mostrem com a màxim 10 files per microbatch.
        .option("numRows", 10)

        # Definim que la consulta s'executi cada 10 segons.
        # Cada 10 segons Spark crea un nou microbatch.
        .trigger(processingTime="10 seconds")

        # Iniciem la consulta streaming.
        .start()
    )

    # Mantenim la consulta activa fins que l'aturem manualment.
    # En una aplicació streaming això és normal: el programa no acaba sol.
    query.awaitTermination()

except KeyboardInterrupt:
    # Si aturem l'execució amb CTRL + C, tanquem correctament
    # la consulta streaming, la SparkSession i el SparkContext.
    query.stop()
    spark.stop()
    sc.stop()