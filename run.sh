#!/bin/bash

# Installation des packages Python nécessaires
echo "Installation des packages Python requis"
pip install -r requirements.txt

## En local ->
## pig -x local -

## en dataproc...
echo "Création du bucket Google Cloud Storage"
gsutil mb -l europe-west3 gs://bucket_pagerank2024/

echo "Création du dossier de données local"
mkdir -p ~/data/

echo "Vérification de l'existence des fichiers de données dans le dossier local"
if [ -f ~/data/small_page_links.nt ]; then
  echo "Le fichier small_page_links.nt existe. Prêt pour la copie."
else
  echo "Le fichier small_page_links.nt est introuvable. Téléchargement en cours."
  gsutil -m cp -r gs://public_lddm_data/* ~/data/
fi

# Copie des fichiers de données dans le bucket Google Cloud Storage
echo "Copie des fichiers de données dans le bucket"
if [ -f ~/data/small_page_links.nt ]; then
  gsutil cp ~/data/small_page_links.nt gs://bucket_pagerank2024/
  echo "Copie réussie."
else
  echo "Erreur : Le fichier small_page_links.nt n'a pas été trouvé après le téléchargement."
  exit 1
fi

## copy pig code
echo "Copie du code PySpark dans le bucket"
gsutil cp main/pypagerank.py gs://bucket_pagerank2024/
gsutil cp main/pypagerank_with_url_partionner.py gs://bucket_pagerank2024/
gsutil cp main/pypagerank_dataframe.py gs://bucket_pagerank2024/
gsutil cp main/pypagerank_dataframe_with_url_partionner.py gs://bucket_pagerank2024/

# Suppression du répertoire de sortie si existant
gsutil rm -rf gs://bucket_pagerank2024/out/ 

# Boucle sur les configurations de clusters
for num_workers in 0 2 4; do

  # Création du cluster avec le nombre de nœuds spécifié
  echo "Création du cluster avec $num_workers nœuds"
  gcloud dataproc clusters create "cluster-${num_workers}-nodes" \
    --enable-component-gateway \
    --region europe-west3 \
    --zone europe-west3-c \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers $num_workers \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project pangerank2024
  
  echo "Soumission du job PySpark sur le cluster cluster-${num_workers}-nodes"

  # Exécution du job PySpark
  echo "execute pypagerank.py"
  gcloud dataproc jobs submit pyspark \
    --region europe-west3 \
    --cluster "cluster-${num_workers}-nodes" \
    gs://bucket_pagerank2024/pypagerank.py -- gs://bucket_pagerank2024/small_page_links.nt 3

  echo "execute pypagerank_with_url_partionner.py"
  gcloud dataproc jobs submit pyspark \
    --region europe-west3 \
    --cluster "cluster-${num_workers}-nodes" \
    gs://bucket_pagerank2024/pypagerank_with_url_partionner.py -- gs://bucket_pagerank2024/small_page_links.nt 3

  echo "execute pypagerank_dataframe.py"
  gcloud dataproc jobs submit pyspark \
    --region europe-west3 \
    --cluster "cluster-${num_workers}-nodes" \
    gs://bucket_pagerank2024/pypagerank_dataframe.py -- gs://bucket_pagerank2024/small_page_links.nt 3

  echo "execute pypagerank_dataframe_with_url_partionner.py"
  gcloud dataproc jobs submit pyspark \
    --region europe-west3 \
    --cluster "cluster-${num_workers}-nodes" \
    gs://bucket_pagerank2024/pypagerank_dataframe_with_url_partionner.py -- gs://bucket_pagerank2024/small_page_links.nt 3
  
  # delete cluster...
  echo "Suppression du cluster cluster-${num_workers}-nodes"

  gcloud dataproc clusters delete "cluster-${num_workers}-nodes" --region europe-west3 --quiet

  # gsutil cat gs://bucket_pagerank2024/out/pagerank_data_spark_partitioned/part-00000 | head -n 5

  echo "Cluster cluster-${num_workers}-nodes supprimé"

done

echo "Copie du output bucket en local"
gsutil cp -r gs://bucket_pagerank2024/out/ /home/nicolas_stucky0/PageRank2024/

echo "Script terminé"