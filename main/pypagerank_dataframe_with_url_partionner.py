import findspark

file_name = "small_page_links.nt"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName("PythonPageRank") \
  .getOrCreate()

# !wget -q https://storage.googleapis.com/public_lddm_data/small_page_links.nt
# !ls

df_text = spark.read.text("gs://bucket_pagerank2024/"+file_name)

from pyspark.sql.functions import split

df = df_text.select(split(df_text.value, "\s+").alias("columns"))
df = df.selectExpr("columns[0] as subject", "columns[1] as predicate", "columns[2] as object")

# df.head(5)

import re
def computeContribs(urls, rank) :
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


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

# Ajout du partitionnement par URL (exemple de partitionnement par 'source')
num_partitions = 4
links_df = links_df.repartition(num_partitions, "source")

links_df = links_df.cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks_df = links_df.select("source").distinct().withColumn("rank", F.lit(1.0))

# links_df.head(5)

links_df = links_df.withColumn("count", F.size("neighbors"))
links_df = links_df.sort(F.col("count").desc())
# links_df.select("source", "count").head(10)

# ranks_df.head(5)

joined_df = links_df.join(ranks_df, on="source", how="inner")
joined_df = joined_df.withColumn("neighbor_count", F.size("neighbors"))
# joined_df.head(5)

contrib_df = joined_df.select(F.explode("neighbors").alias("source"), (F.col("rank") / F.col("neighbor_count")).alias("contrib"))

# contrib_df.head(5)

for iteration in range(1):
  # Calculates URL contributions to the rank of other URLs.
  contrib_df = joined_df.select(F.explode("neighbors").alias("source"), (F.col("rank") / F.col("neighbor_count")).alias("contrib"))

  # Re-calculates URL ranks based on neighbor contributions.
  ranks_df = contrib_df.groupBy("source").agg(F.sum("contrib").alias("total_contrib"))
  ranks_df = ranks_df.withColumn("rank", ranks_df["total_contrib"] * 0.85 + 0.15)

  ranks_df = ranks_df.drop("total_contrib")

  joined_df = joined_df.drop("rank").join(ranks_df, on="source", how="inner")

end_time = time.time()
execution_time = end_time - start_time

# for row in ranks_df.collect():
#     print("%s has rank: %s." % (row["source"], row["rank"]))

print(f"Temps d'exécution : {execution_time} secondes")

ranks_df.select("source", "rank").rdd.map(lambda row: f"{row['source']}\t{row['rank']}").saveAsTextFile("gs://bucket_pagerank2024/out/ranks_dataframe_with_partionner_parquet")