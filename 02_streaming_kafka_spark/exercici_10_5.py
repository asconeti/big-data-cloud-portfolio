import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row

import json
import re
import html

# Objectiu:
# Crear una taula resum que s'actualitzi cada 5 segons.
#
# La taula ha de calcular informació sobre els toots originals rebuts
# durant una finestra temporal de 60 segons.
#
# Columnes demanades:
# - lang: idioma
# - num_toots: nombre de toots originals en aquell idioma
# - avg_len_content: longitud mitjana del contingut del toot
# - user: usuari amb més seguidors dins d'aquell idioma
# - followers: nombre de seguidors d'aquest usuari
# -------------------------------------------------------------------


# Nom de l'aplicació Spark.
app_name = "exercici_10_5_powering_up"


# -------------------------------------------------------------------
# Creació del SparkContext
# -------------------------------------------------------------------
# SparkContext és el punt d'entrada principal de Spark.
# Fem servir local[*] perquè Spark utilitzi tots els nuclis disponibles.
try:
    sc = SparkContext("local[*]", appName=app_name)

except ValueError:
    # Si ja hi hagués un SparkContext actiu, l'aturem i en creem un de nou.
    sc.stop()
    sc = SparkContext("local[*]", appName=app_name)


# Reduïm els missatges de log perquè la sortida sigui més llegible.
sc.setLogLevel("ERROR")


# -------------------------------------------------------------------
# Creació del StreamingContext
# -------------------------------------------------------------------
# L'enunciat demana que la taula s'actualitzi cada 5 segons.
# Per això fem servir un batch interval de 5 segons.
batch_interval = 5

# StreamingContext és el punt d'entrada de Spark Streaming.
ssc = StreamingContext(sc, batch_interval)


# Definim un directori de checkpoint.
# És recomanable en operacions amb finestra perquè Spark pot necessitar
# guardar informació intermèdia del càlcul.
ssc.checkpoint("checkpoint_10_5")


# -------------------------------------------------------------------
# Definició del flux d'entrada
# -------------------------------------------------------------------
# El flux de Mastodon està disponible al clúster a través del socket.
socket_host = "localhost"
socket_port = 9999

# Creem un DStream que llegeix línies de text del socket.
# Cada línia rebuda és un JSON en format text.
kafkaStream = ssc.socketTextStream(socket_host, socket_port)


# -------------------------------------------------------------------
# SparkSession reutilitzable
# -------------------------------------------------------------------
# L'exemple del document de Spark Streaming recomana crear una
# SparkSession de manera reutilitzable dins de foreachRDD().
#
# SparkSession ens permet treballar amb DataFrames i SQL.
def getSparkSessionInstance(sparkConf):

    # Comprovem si ja existeix una SparkSession global.
    if "sparkSessionSingletonInstance" not in globals():

        # Si no existeix, la creem a partir de la configuració de Spark.
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()

    # Retornem la SparkSession existent o acabada de crear.
    return globals()["sparkSessionSingletonInstance"]


# -------------------------------------------------------------------
# Funció per netejar el text del toot
# -------------------------------------------------------------------
# El camp "content" de Mastodon pot contenir HTML.
# Per exemple: <p>text</p>, &amp;, &quot;, etc.
#
# Aquesta funció elimina les etiquetes HTML i converteix entitats HTML
# a text normal.
def clean_text(content):

    # Eliminem etiquetes HTML com <p>, <br>, <a>, etc.
    text = re.sub("<[^>]+>", "", content)

    # Convertim entitats HTML com &amp; en &, &quot; en ", etc.
    text = html.unescape(text)

    # Retornem el text net.
    return text


# -------------------------------------------------------------------
# Funció per interpretar cada toot
# -------------------------------------------------------------------
# Aquesta funció rep una línia JSON en format text.
# La converteix en diccionari Python i extreu només els camps necessaris.
#
# Si el toot no és vàlid, no és original o no té idioma, retornem None.
def parse_toot(x):

    try:
        # Convertim la línia rebuda pel socket en un diccionari Python.
        toot = json.loads(x.strip())

        # L'enunciat demana només toots originals.
        # Si "reblog" no és None, vol dir que és un retoot.
        if toot.get("reblog") is not None:
            return None

        # Recuperem l'idioma del toot.
        lang = toot.get("language")

        # Si no hi ha idioma, descartem el registre.
        if lang is None:
            return None

        # Recuperem el contingut del toot.
        # En Mastodon normalment és "content", però deixem "text"
        # com a alternativa per seguretat.
        content = toot.get("content") or toot.get("text") or ""

        # Netegem el contingut HTML.
        content = clean_text(content)

        # Calculem la longitud del contingut en caràcters.
        content_len = len(content)

        # Recuperem la informació de l'usuari.
        # En Mastodon normalment el camp és "account".
        account = toot.get("account") or toot.get("user") or {}

        # Recuperem el nom d'usuari.
        # Provem diversos camps possibles i, si no n'hi ha cap,
        # assignem "unknown".
        user = (
            account.get("acct")
            or account.get("username")
            or account.get("display_name")
            or "unknown"
        )

        # Recuperem el nombre de seguidors.
        followers = account.get("followers_count") or 0

        # Convertim els seguidors a enter.
        followers = int(followers)

        # Retornem una Row.
        # Aquesta Row serà una fila del futur DataFrame.
        return Row(
            lang=lang,
            content_len=content_len,
            user=user,
            followers=followers
        )

    except Exception:
        # Si algun registre ve mal format o no es pot processar,
        # el descartem retornant None.
        return None


# -------------------------------------------------------------------
# Funció per processar cada RDD de la finestra
# -------------------------------------------------------------------
# Aquesta funció s'executa a cada actualització de la finestra.
#
# Rep:
# - time: instant temporal del batch
# - rdd: dades de la finestra actual
#
# Dins d'aquesta funció convertim l'RDD en DataFrame i fem SQL.
def process(time, rdd):

    # Mostrem el temps del batch per identificar cada sortida.
    print("\n========= %s =========" % str(time))

    # Si l'RDD està buit, no podem crear cap DataFrame.
    if rdd.isEmpty():
        print("No hi ha dades en aquesta finestra.")
        return

    try:
        # Obtenim o creem la SparkSession.
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convertim l'RDD de Row en un DataFrame.
        # El DataFrame tindrà les columnes:
        # lang, content_len, user, followers.
        tootsDataFrame = spark.createDataFrame(rdd)

        # Creem una vista temporal anomenada "toots".
        # Això ens permet escriure consultes SQL sobre el DataFrame.
        tootsDataFrame.createOrReplaceTempView("toots")

        # Consulta SQL:
        #
        # 1. aggregated:
        #    Agrupa per idioma i calcula:
        #    - nombre de toots
        #    - longitud mitjana del contingut
        #
        # 2. ranked_users:
        #    Ordena els usuaris de cada idioma segons el nombre de seguidors.
        #    ROW_NUMBER() assigna rn = 1 a l'usuari amb més seguidors.
        #
        # 3. Consulta final:
        #    Uneix les mètriques agregades amb l'usuari més seguit.
        #    Ordena per nombre de toots i limita la sortida a 10 files.
        resultDataFrame = spark.sql("""
            WITH aggregated AS (
                SELECT
                    lang,
                    COUNT(*) AS num_toots,
                    ROUND(AVG(content_len), 2) AS avg_len_content
                FROM toots
                GROUP BY lang
            ),
            ranked_users AS (
                SELECT
                    lang,
                    `user`,
                    followers,
                    ROW_NUMBER() OVER (
                        PARTITION BY lang
                        ORDER BY followers DESC, `user` ASC
                    ) AS rn
                FROM toots
            )
            SELECT
                a.lang,
                a.num_toots,
                a.avg_len_content,
                r.`user`,
                r.followers
            FROM aggregated a
            JOIN ranked_users r
                ON a.lang = r.lang
            WHERE r.rn = 1
            ORDER BY a.num_toots DESC, a.lang ASC
            LIMIT 10
        """)

        # Mostrem la taula final.
        # truncate=False evita que Spark retalli noms d'usuari llargs.
        resultDataFrame.show(10, truncate=False)

    except Exception as e:
        # Si hi ha algun problema en aquesta finestra,
        # mostrem l'error però no aturem tota l'aplicació.
        print("Error processant aquesta finestra:", e)


# -------------------------------------------------------------------
# Construcció del flux principal
# -------------------------------------------------------------------
# Ara definim les transformacions sobre el DStream.
tootRows = (
    kafkaStream

    # Convertim cada línia JSON en una Row amb les dades necessàries.
    .map(parse_toot)

    # Eliminem els registres descartats per parse_toot().
    .filter(lambda x: x is not None)

    # Apliquem una finestra de 60 segons.
    # El segon paràmetre indica que s'actualitza cada 5 segons.
    .window(60, 5)
)


# -------------------------------------------------------------------
# Sortida del flux
# -------------------------------------------------------------------
# foreachRDD aplica la funció process() a cada RDD generat pel DStream.
# És aquí on es força l'execució del flux i es mostra la taula.
tootRows.foreachRDD(process)


# -------------------------------------------------------------------
# Execució de Spark Streaming
# -------------------------------------------------------------------
try:
    # Iniciem el procés de streaming.
    ssc.start()

    # Mantenim l'aplicació en execució fins que l'aturem manualment.
    ssc.awaitTermination()

except KeyboardInterrupt:
    # Si aturem amb CTRL + C, tanquem StreamingContext i SparkContext.
    ssc.stop()
    sc.stop()