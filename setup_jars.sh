#!/usr/bin/env bash
# Setup JARs for Spark Kafka+Postgres detector via Maven
# Uses Maven to handle transitive dependencies properly

echo "[jars] Downloading Maven dependencies for Kafka-Spark connector..."

# Use maven directly to download all dependencies  
mvn dependency:copy-dependencies \
  -DoutputDirectory=jars \
  -Dartifact=org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  -Dartifact=org.postgresql:postgresql:42.7.1 2>&1 | grep -E "(Downloading|Downloaded|BUILD)"

if [ -d "jars" ]; then
  echo "[ok] Downloaded $(ls jars/*.jar 2>/dev/null | wc -l) JAR files"
  du -sh jars
else
  echo "[error] Maven failed to create jars directory"
  exit 1
fi
