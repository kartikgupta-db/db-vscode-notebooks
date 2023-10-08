# Databricks notebook source
dbutils.widgets.text("limit", "10")
dbutils.widgets.text("data_source_url", "")
# COMMAND ----------

# MAGIC %md
# MAGIC # Download and analyse external data

# COMMAND ----------

data_source_url=dbutils.widgets.get("data_source_url")
# COMMAND ----------

# MAGIC %run ./download_dataset 

# COMMAND ----------

df.describe()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

limit = int(dbutils.widgets.get("limit"))
# Top 10 artists with most songs in the dataset
top_artists = df.groupBy("artist(s)_name").count().orderBy(F.desc("count")).limit(limit).toPandas()

# Plot
plt.figure(figsize=(12, 6))
sns.barplot(x=top_artists["artist(s)_name"], y=top_artists["count"], palette='viridis')
plt.xlabel('Number of Songs')
plt.ylabel('Artist(s) Name')
plt.title('Top 10 Artists with Most Songs')
plt.show()

top_artists

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse data already in databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips
# MAGIC order by trip_distance desc

# COMMAND ----------

limit = int(dbutils.widgets.get("limit"))
df = _sqldf.limit(limit)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

limit = int(dbutils.widgets.get("limit"))
df = _sqldf.select("trip_distance").toPandas()

# Plot
plt.figure(figsize=(12, 6))
sns.histplot(data=df, palette='viridis')
plt.xlabel('Trip distance')
plt.ylabel('# of trups')
plt.title('Distribution of trips')
plt.show()

# COMMAND ----------


