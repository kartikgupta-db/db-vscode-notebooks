# Databricks notebook source


# COMMAND ----------

import subprocess
import os
import argparse
import sys
import pandas as pd

df = pd.read_csv(data_source_url, encoding='ISO-8859-1')
df = spark.createDataFrame(df)
df.show()
