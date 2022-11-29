# Databricks notebook source
class Constant:
  SPARK_SHUFFLE_PARTITION = 8
  YMD_HMS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
  EXPECTED_TABLE_NAMESPACE_PARTS = 3
  BRONZE_CONTAINER = "/mnt/bronze"
  SILVER_CONTAINER = "/mnt/silver"
  GOLD_CONTAINER = "/mnt/gold"
  DELETE_TABLE_SUFFIX = "_deleted"
  # Tried different no. of partition to optimize the performance of spark job.
  # It is giving best performance with 24 partitions.
  PARTITION = 24
  GOLD_LAYER_SERVER = "jdbc:sqlserver://toromont.database.windows.net:1433;databaseName="
  GOLD_LAYER_DB = "devanalyticscallcenter"
  GOLD_LAYER_SCHEMA = "dbo"
