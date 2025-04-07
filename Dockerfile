# Utiliser une image de base avec Python et Java (pour Spark)
FROM python:3.11-slim

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers requirements
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Installer Spark manuellement
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/ \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Définir les variables d'environnement pour Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Copier le script de benchmark
COPY benchmark.py .

# Copier les données (optionnel, si elles sont générées à l'extérieur)
COPY data/ ./data/

# Commande par défaut pour lancer le benchmark
CMD ["python", "benchmark.py"]
