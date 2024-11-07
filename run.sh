#!/bin/bash

# Installation des packages Python nécessaires
pip install -r requirements.txt

## En local ->
## pig -x local -

## en dataproc...

## copy data
gsutil cp small_page_links.nt gs://myown_bucket/

## copy pig code
gsutil cp pagerank-notype.py gs://myown_bucket/

## Clean out directory
gsutil rm -rf gs://myown_bucket/out


# Boucle sur les configurations de clusters
for num_workers in 1 2 4; do
  # Création du cluster avec le nombre de nœuds spécifié
  gcloud dataproc clusters create "cluster-${num_workers}-nodes" \
    --enable-component-gateway \
    --region europe-west3 \
    --zone europe-west3-a \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers $num_workers \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project master-2-large-scale-data
  
  # Exécution du job PySpark
  gcloud dataproc jobs submit pyspark \
    --region europe-west3 \
    --cluster "cluster-${num_workers}-nodes" \
    gs://myown_bucket/pagerank-notype.py -- gs://myown_bucket/small_page_links.nt 3
  
## access results
gsutil cat gs://myown_bucket/out/pagerank_data_10/part-r-00000

## delete cluster...
gcloud dataproc clusters delete "cluster-${num_workers}-nodes" --region europe-west3 --quiet