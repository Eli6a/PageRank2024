#!/bin/bash

# Variables à changer
bucket="gs://eli_bucket_pagerank2024/"
text_file="page_links_en.nt.bz2"
project_id="page-rank-440808"
git_folder_path="/home/sisa_elis/PageRank2024/"

# Installation des packages Python nécessaires
echo "Installation des packages Python requis"
pip install -r requirements.txt


## en dataproc...
echo "Création du bucket Google Cloud Storage"
gsutil mb -l europe-west3 "${bucket}"

echo "Création du dossier de données local"
mkdir -p ${git_folder_path}data/

echo "Vérification de l'existence des fichiers de données dans le dossier local"
if [ -f "${git_folder_path}data/${text_file}" ]; then
  echo "Le fichier ${text_file} existe. Prêt pour la copie."
else
  echo "Le fichier ${text_file} est introuvable. Téléchargement en cours."
  gsutil -m cp -r gs://public_lddm_data/* ${git_folder_path}data/
fi

# Copie des fichiers de données dans le bucket Google Cloud Storage
echo "Copie des fichiers de données dans le bucket"
if [ -f "${git_folder_path}data/${text_file}" ]; then
  gsutil cp "${git_folder_path}data/${text_file}" "${bucket}"
  echo "Copie réussie."
else
  echo "Erreur : Le fichier ${text_file} n'a pas été trouvé après le téléchargement."
  exit 1
fi

## copy code
echo "Copie du code PySpark dans le bucket"
gsutil cp main/pypagerank.py "${bucket}"
gsutil cp main/pypagerank_with_url_partionner.py "${bucket}"
gsutil cp main/pypagerank_dataframe.py "${bucket}"
gsutil cp main/pypagerank_dataframe_with_url_partionner.py "${bucket}"

# Réinitialisation du répertoire de sortie si existant
gsutil rm -rf "${bucket}out/"
gsutil cp /dev/null "${bucket}out/"

# Créer ou vide execution-time.txt
# echo "" > "${git_folder_path}execution-time.txt"

# Boucle sur les configurations de clusters
for num_workers in 0 2 4; do
  if [ "$num_workers" -eq 0 ]; then
    gcloud dataproc clusters create "cluster-${num_workers}-nodes" \
      --enable-component-gateway \
      --region europe-west3 \
      --master-machine-type n1-highmem-8 \
      --master-boot-disk-size 500 \
      --num-workers "$num_workers" \
      --image-version 2.0-debian10 \
      --project "$project_id"
  else
    echo "Creating cluster with $num_workers nodes"
    gcloud dataproc clusters create "cluster-${num_workers}-nodes" \
      --enable-component-gateway \
      --region europe-west3 \
      --master-machine-type n1-highmem-8 \
      --master-boot-disk-size 500 \
      --num-workers "$num_workers" \
      --worker-machine-type n1-standard-4 \
      --worker-boot-disk-size 500 \
      --image-version 2.0-debian10 \
      --project "$project_id"
  fi
    
  echo "Soumission du job PySpark sur le cluster cluster-${num_workers}-nodes"
  
  # Boucle sur les scripts pythons
  for script in pypagerank_dataframe.py pypagerank_dataframe_with_url_partionner.py; do # pypagerank.py pypagerank_with_url_partionner.py
    script_name=$(basename "$script" .py)
    output_path="${bucket}out/${script_name}-${num_workers}"

    start_time=$(date +%s.%N)

    # Exécution du job PySpark
    echo "Executing $script_name"
    gcloud dataproc jobs submit pyspark \
      --region europe-west3 \
      --cluster "cluster-${num_workers}-nodes" \
      --properties=spark.executor.memory=10g,spark.executor.cores=4,spark.driver.memory=10g,spark.driver.cores=4 \
      "${bucket}${script}" -- "${bucket}${text_file}" 3 "$output_path"

    end_time=$(date +%s.%N)
    execution_time=$(awk "BEGIN {print $end_time - $start_time}")

    # Append result to the execution-time.txt file
    echo "('${script_name}-${num_workers}', $execution_time)" >> "${git_folder_path}execution-time.txt"
  
  done
  
  # delete cluster...
  echo "Suppression du cluster cluster-${num_workers}-nodes"

  gcloud dataproc clusters delete "cluster-${num_workers}-nodes" --region europe-west3 --quiet

  # gsutil cat gs://bucket_pagerank2024/out/pagerank_data_spark_partitioned/part-00000 | head -n 5

  echo "Cluster cluster-${num_workers}-nodes supprimé"

done

echo "Copie du output bucket en local"
gsutil cp -r "${bucket}out/" "${git_folder_path}"

echo "Script terminé"