# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists watermark_db location "/mnt/silver/watermark_db";
# MAGIC create database if not exists appdb_equiplink location "/mnt/silver/appdb_equiplink";
# MAGIC create database if not exists appdb_saleslink location "/mnt/silver/appdb_saleslink";
# MAGIC create database if not exists corporatedb location "/mnt/silver/corporatedb";
# MAGIC create database if not exists dwdb location "/mnt/silver/dwdb";
# MAGIC create database if not exists xdb location "/mnt/silver/xdb";
# MAGIC create database if not exists toromontcatdb location "/mnt/silver/toromontcatdb";
# MAGIC create database if not exists ccmdata location "/mnt/silver/ccmdata";

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists watermark_db.watermark_table
# MAGIC (
# MAGIC   table_name string,
# MAGIC   watermark_value timestamp
# MAGIC )
# MAGIC USING DELTA
