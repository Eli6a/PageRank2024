import sys

input_path = sys.argv[1]
output_path = sys.argv[3]

import findspark

findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .appName("PythonPageRank") \
  .getOrCreate()

lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
lines.take(5)

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

# Loads all URLs from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

import time
start_time = time.time()
# Partitionnement des URLs dans un nombre de partitions donné
num_partitions = 16
partitioned_links = links.partitionBy(num_partitions, lambda url: hash(url))

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

from operator import add
for iteration in range(int(sys.argv[2])):
  # Calculates URL contributions to the rank of other URLs.
  contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

  # Re-calculates URL ranks based on neighbor contributions.
  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

end_time = time.time()
execution_time = end_time - start_time
print(f"Temps d'exécution : {execution_time} secondes")

ranks.persist()
ranks.saveAsTextFile(output_path)
