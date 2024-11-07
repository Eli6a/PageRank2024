import findspark

findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# !wget -q https://storage.googleapis.com/public_lddm_data/small_page_links.nt
# !ls

with open("data/small_page_links.nt", "r") as file:
    lines = file.readlines()

split_lines = [line.strip().split() for line in lines]

parsed_data = [(str(triple[0]), str(triple[1]), str(triple[2])) for triple in split_lines]
df = spark.createDataFrame(parsed_data, schema='subject string, predicate string, object string')

df.head(5)

import re
def computeContribs(urls, rank) :
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls) if num_urls > 0 else (url, 0)

def parseNeighbors(urls) :
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

from pyspark.sql import functions as F

# Loads all URLs from input file and initialize their neighbors.
links_df = df.select(df[0].alias("source"), df[2].alias("neighbor"))
links_df = links_df.distinct()
links_df = links_df.groupBy("source").agg(F.collect_list("neighbor").alias("neighbors"))

import time
start_time = time.time()

links_df = links_df.cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks_df = links_df.select("source").distinct().withColumn("rank", F.lit(1.0))

links_df.head(5)

links_df = links_df.withColumn("count", F.size("neighbors"))
links_df = links_df.sort(F.col("count").desc())
links_df.select("source", "count").head(10)

ranks_df.head(5)

joined_df = links_df.join(ranks_df, on="source", how="inner")
joined_df.head(5)

contrib = [(url, contrib) for row in joined_df.collect() for url, contrib in computeContribs(row["neighbors"], row["rank"])]
contrib_df = spark.createDataFrame(contrib, schema='source string, contrib float')

contrib_df.head(5)

for iteration in range(1):
  # Calculates URL contributions to the rank of other URLs.
  contrib = [(url, contrib) for row in joined_df.collect() for url, contrib in computeContribs(row["neighbors"], row["rank"])]
  contrib_df = spark.createDataFrame(contrib, schema='source string, contrib float')

  # Re-calculates URL ranks based on neighbor contributions.
  ranks_df = contrib_df.groupBy("source").agg(F.sum("contrib").alias("total_contrib"))
  ranks_df = ranks_df.withColumn("rank", ranks_df["total_contrib"] * 0.85 + 0.15)

  ranks_df = ranks_df.drop("total_contrib")

  joined_df = joined_df.drop("rank").join(ranks_df, on="source", how="left")

  # Collects all URL ranks and dump them to console.

end_time = time.time()
execution_time = end_time - start_time

for row in ranks_df.collect():
    print("%s has rank: %s." % (row["source"], row["rank"]))

print(f"Temps d'exécution : {execution_time} secondes")