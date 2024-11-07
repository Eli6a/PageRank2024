#!/bin/bash

# Installation des packages Python nécessaires
pip install -r requirements.txt

## En local ->
## pig -x local -

## en dataproc...

gsutil mb -l europe-west3 gs://bucket_pagerank2024/

mkdir -p ~/data/

gsutil -m cp -r gs://public_lddm_data/* ~/data/

## copy data
gsutil cp small_page_links.nt gs://bucket_pagerank2024/

## copy pig code
gsutil cp pypagerank.py gs://bucket_pagerank2024/

# # Boucle sur les configurations de clusters
# for num_workers in 1 2 4; do
#   # Création du cluster avec le nombre de nœuds spécifié
#   gcloud dataproc clusters create "cluster-${num_workers}-nodes" \
#     --enable-component-gateway \
#     --region europe-west3 \
#     --zone europe-west3-a \
#     --master-machine-type n1-standard-4 \
#     --master-boot-disk-size 500 \
#     --num-workers $num_workers \
#     --worker-machine-type n1-standard-4 \
#     --worker-boot-disk-size 500 \
#     --image-version 2.0-debian10 \
#     --project master-2-large-scale-data
  
#   # Exécution du job PySpark
#   gcloud dataproc jobs submit pyspark \
#     --region europe-west3 \
#     --cluster "cluster-${num_workers}-nodes" \
#     gs://bucket_pagerank2024/pagerank-notype.py -- gs://bucket_pagerank2024/small_page_links.nt 3
  
# ## delete cluster...
# gcloud dataproc clusters delete "cluster-${num_workers}-nodes" --region europe-west3 --quiet