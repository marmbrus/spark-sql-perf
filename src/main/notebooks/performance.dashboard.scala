// Databricks notebook source exported at Sun, 13 Sep 2015 07:08:06 UTC
import com.databricks.spark.sql.perf._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

sqlContext.read.load("/databricks/spark/sql/sqlPerformanceCompact").registerTempTable("sqlPerformanceCompact")

val timestampWindow = Window.partitionBy("sparkVersion").orderBy($"timestamp".desc)

val normalizeVersion = udf((v: String) => v.stripSuffix("-SNAPSHOT"))

class RecentRuns(prefix: String, inputFunction: DataFrame => DataFrame) {
  val inputData = table("sqlPerformanceCompact") 
    .where($"tags.runtype" === "daily")
    .withColumn("sparkVersion", normalizeVersion($"configuration.sparkVersion"))
    .withColumn("startTime", from_unixtime($"timestamp" / 1000))
    .withColumn("database", $"tags.database")
    .withColumn("jobname", $"tags.jobname")
  
  val recentRuns = inputFunction(inputData)
    .withColumn("runId", denseRank().over(timestampWindow))
    .withColumn("runId", -$"runId")
    .filter($"runId" >= -10) 

  val baseData = recentRuns
    .withColumn("result", explode($"results"))
    .withColumn("day", concat(month($"startTime"), lit("-"), dayofmonth($"startTime"))) 
    .withColumn("runtimeSeconds", runtime / 1000)
    .withColumn("queryName", $"result.name")

  baseData.registerTempTable(s"${prefix}_baseData")

  val smoothed = baseData
    .where($"iteration" !== 1)
    .groupBy("runId", "sparkVersion", "database", "queryName")
    .agg(callUDF("percentile", $"runtimeSeconds".cast("long"), lit(0.5)).as('medianRuntimeSeconds))
    .orderBy($"runId", $"sparkVersion")

  smoothed.registerTempTable(s"${prefix}_smoothed")
}

val tpcds1500 = new RecentRuns("tpcds1500", _.filter($"database" === "tpcds1500"))

// COMMAND ----------

display(tpcds1500.recentRuns.select($"runId", $"sparkVersion", $"startTime", $"timestamp").distinct)

// COMMAND ----------

class GeometricMean extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("product", DoubleType) :: Nil
  )
  
  def dataType: DataType = DoubleType
  
  def deterministic: Boolean = true
  
  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
  
  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }
  
  def inputSchema: org.apache.spark.sql.types.StructType = 
    StructType(StructField("value", DoubleType) :: Nil)
  
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }
  
  def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }
}

val gm = new GeometricMean
sqlContext.udf.register("gm", gm)

// COMMAND ----------

// MAGIC %python spark_versions = ["1.6.0", "1.5.0", "1.4.1"]
// MAGIC 
// MAGIC import matplotlib.pyplot as plt
// MAGIC import numpy as np
// MAGIC from math import ceil, floor
// MAGIC 
// MAGIC def plot_single(df, column, height = 30):
// MAGIC   for v in spark_versions:
// MAGIC     runtimes = df[df['sparkVersion'] == v][column]
// MAGIC     padded = ([0.0] * (10 - len(runtimes))) + list(runtimes)
// MAGIC     plt.plot(range(10), padded)
// MAGIC 
// MAGIC   plt.xticks(range(10), list(df[df['sparkVersion'] == v]['runId']))
// MAGIC 
// MAGIC   plt.legend(spark_versions, loc='lower left')
// MAGIC   plt.ylim([0,height])
// MAGIC 
// MAGIC 
// MAGIC def bar_chart(df, graphCol, groupCol, colorCol, heightCol, colors = ['#29B1C1', '#EF6C30', '#21B16B', '#E8302D']):
// MAGIC   graphs = df[graphCol].unique()
// MAGIC   num_graphs = len(graphs)
// MAGIC   rows = int(ceil(num_graphs / 2))
// MAGIC 
// MAGIC   fig, axarr = plt.subplots(num_graphs, figsize=(5, 15))
// MAGIC 
// MAGIC   for idx, graph in enumerate(graphs):
// MAGIC     ax = axarr[idx]
// MAGIC     data = df[df[graphCol] == graph]
// MAGIC 
// MAGIC     ax.set_title(graph)
// MAGIC     groups = list(data[groupCol].unique())
// MAGIC     num_groups = len(groups)
// MAGIC     width = 0.3                  # the width of the bars
// MAGIC     ind = np.arange(num_groups)  # the x locations for the groups
// MAGIC     ax.set_xticklabels(groups)
// MAGIC     ax.set_xticks(np.arange(num_groups) + (width * 1.5))
// MAGIC 
// MAGIC     color_values = sorted(list(df[colorCol].unique()))
// MAGIC     rects = []
// MAGIC     for i, color in enumerate(color_values):
// MAGIC       data = df[df[colorCol] == color][df[graphCol] == graph]
// MAGIC       print colors[i]
// MAGIC       rect = ax.bar(ind, list(data[heightCol]), width, color=colors[i], yerr=list(data['error']), ecolor='black')
// MAGIC       rects.append(rect)
// MAGIC       ind = ind + width
// MAGIC 
// MAGIC     ax.legend( rects, color_values, loc='upper left')
// MAGIC 
// MAGIC   display(fig)

// COMMAND ----------

val groups = 
  ("Interactive Queries", Seq("q19", "q42", "q52", "q55", "q63", "q68", "q73", "q98")) ::
  ("Reporting Queries", Seq("q3","q7","q27","q43", "q53", "q89")) ::
  ("Deep Analytics", Seq("q34", "q46", "q59", "q65",  "q79", "ss_max")) :: Nil

groups.toDF("queryType", "queries").select($"queryType", explode($"queries").as("queryName")).registerTempTable("queryTypes")

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from pyspark.sql.functions import expr
// MAGIC 
// MAGIC df = table("tpcds1500_smoothed").alias("data") \
// MAGIC   .join(table("queryTypes"), expr("queryTypes.queryName = data.queryName")) \
// MAGIC   .groupBy("queryType", "data.queryName", "sparkVersion") \
// MAGIC   .agg( \
// MAGIC     expr("avg(medianRuntimeSeconds) AS score"), \
// MAGIC     expr("stddev(medianRuntimeSeconds) as error")) \
// MAGIC   .orderBy("data.queryName") \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC #display(df)
// MAGIC bar_chart(df, graphCol = 'queryType', groupCol='queryName', colorCol='sparkVersion', heightCol='score')

// COMMAND ----------

// MAGIC %md ## TPCDS - 20 Queries, Total Runtime (Smoothed to reduce outliers)
// MAGIC  -  time for each query = median of 4 iterations
// MAGIC  - dropping 1 warmup iteration

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import numpy as np
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC df = table("tpcds1500_smoothed") \
// MAGIC   .groupBy("runId", "sparkVersion") \
// MAGIC   .agg(sum("medianRuntimeSeconds").alias("totalRunTime")) \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "totalRunTime", height=1000)
// MAGIC display(fig)

// COMMAND ----------

org.apache.spark.SPARK_VERSION

// COMMAND ----------

// MAGIC %python df['sparkVersion'].unique()

// COMMAND ----------

// MAGIC %md ## TPCDS - 20 Queries Geometric Mean
// MAGIC  - time for each query = median of 4 iterations
// MAGIC  - dropping 1 warmup iteration

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC df = table("tpcds1500_smoothed").groupBy("runId", "sparkVersion").agg(expr("gm(medianRuntimeSeconds) AS score")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "score")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC from math import ceil, floor
// MAGIC 
// MAGIC def plot_queries(df, queries):
// MAGIC   num_queries = len(queries)
// MAGIC   rows = int(ceil(num_queries / 2))
// MAGIC 
// MAGIC   for idx, name in enumerate(queries):
// MAGIC     data = df[df['queryName'] == name]
// MAGIC     plt.subplot(rows, 2 , idx)
// MAGIC     plt.title(name)
// MAGIC 
// MAGIC 
// MAGIC     plot_single(data, 'medianRuntimeSeconds', 200)

// COMMAND ----------

// MAGIC %md # Interactive Queries

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q19", "q42", "q52", "q55", "q63", "q68", "q73", "q98"])
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md #Reporting Queries

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q3","q7","q27","q43", "q53", "q89"])
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md # Deep Analytics

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC plot_queries(table("tpcds1500_smoothed").toPandas(), ["q34", "q46", "q59", "q65",  "q79", "ss_max"])
// MAGIC display(fig)

// COMMAND ----------

val joins = new RecentRuns("joins", _.filter($"jobname" === "joins.daily"))
display(joins.recentRuns.select($"runId", $"sparkVersion", $"startTime", $"timestamp").distinct)

// COMMAND ----------

// MAGIC %python
// MAGIC df = table("joins_smoothed").groupBy("runId", "sparkVersion").agg(sum("medianRuntimeSeconds").alias("runtime")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "runtime", 2500)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC df = table("joins_smoothed").groupBy("runId", "sparkVersion").agg(expr("gm(medianRuntimeSeconds) AS score")).toPandas()
// MAGIC 
// MAGIC fig = plt.figure(figsize=(12, 2))
// MAGIC plot_single(df, "score", 200)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(15, 20))
// MAGIC df = table("joins_smoothed").orderBy("queryName").toPandas()
// MAGIC plot_queries(df, list(df['queryName'].unique()))
// MAGIC display(fig)

// COMMAND ----------

import scala.util.Try

val getNum = sqlContext.udf.register("getNum", (q: String) => Try(q.split(": ").last.toInt).getOrElse(0))
val getParam = sqlContext.udf.register("getParam", (q: String) => q.split(": ").last)
val getVariant = sqlContext.udf.register("getVariant", (q: String) => q.split(": ").head.split(" ").last)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df = table("joins_smoothed") \
// MAGIC   .groupBy("queryName", "sparkVersion") \
// MAGIC   .agg( \
// MAGIC     avg("medianRuntimeSeconds").alias("runtime"), \
// MAGIC     expr("stddev(medianRuntimeSeconds)").alias("error")) \
// MAGIC   .withColumn("num", expr("getNum(queryName)")) \
// MAGIC   .withColumn("variant", expr("getVariant(queryName)")) \
// MAGIC   .withColumn("param", expr("getParam(queryName)")) \
// MAGIC   .orderBy("num", "param") \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC bar_chart(df, graphCol = 'variant', groupCol='param', colorCol='sparkVersion')

// COMMAND ----------

display(
  joins
    .baseData
    .where($"sparkVersion" === "1.6.0" && $"queryName" === "join - numMatches: 16")
    .select($"runId", $"iteration", $"runtimeSeconds")
    .orderBy("iteration", "runId"))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df = table("joins_smoothed") \
// MAGIC   .groupBy("queryName", "sparkVersion") \
// MAGIC   .agg( \
// MAGIC     avg("medianRuntimeSeconds").alias("runtime"), \
// MAGIC     expr("stddev(medianRuntimeSeconds)").alias("error")) \
// MAGIC   .withColumn("num", expr("getNum(queryName)")) \
// MAGIC   .withColumn("variant", expr("getVariant(queryName)")) \
// MAGIC   .withColumn("param", expr("getParam(queryName)")) \
// MAGIC   .orderBy("num", "param") \
// MAGIC 
// MAGIC display(df)
// MAGIC df = df.toPandas()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df = table("joins_smoothed") \
// MAGIC   .where("runId >= -1")
// MAGIC   .groupBy("queryName", "sparkVersion") \
// MAGIC   .agg( \
// MAGIC     avg("medianRuntimeSeconds").alias("runtime"), \
// MAGIC     expr("stddev(medianRuntimeSeconds)").alias("error")) \
// MAGIC   .withColumn("num", expr("getNum(queryName)")) \
// MAGIC   .withColumn("variant", expr("getVariant(queryName)")) \
// MAGIC   .withColumn("param", expr("getParam(queryName)")) \
// MAGIC   .orderBy("num", "param") \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC colors = ['r', 'g', 'b']
// MAGIC 
// MAGIC variants = df['variant'].unique()
// MAGIC num_variants = len(variants)
// MAGIC rows = int(ceil(num_variants / 2))
// MAGIC 
// MAGIC #fig = plt.figure()
// MAGIC fig, axarr = plt.subplots(num_variants, figsize=(5, 15))
// MAGIC 
// MAGIC for idx, variant in enumerate(variants):
// MAGIC   ax = axarr[idx]
// MAGIC   data = df[df['variant'] == variant]
// MAGIC 
// MAGIC   ax.set_title(variant)
// MAGIC   nums = list(data['param'].unique())
// MAGIC   N = len(nums)
// MAGIC   width = 0.3       # the width of the bars
// MAGIC   ind = np.arange(N)  # the x locations for the groups
// MAGIC   ax.set_xticklabels( nums )
// MAGIC   ax.set_xticks(np.arange(N) + (width * 1.5))
// MAGIC 
// MAGIC   versions = sorted(list(df['sparkVersion'].unique()))
// MAGIC   rects = []
// MAGIC   for i, v in enumerate(versions):
// MAGIC     data = df[df['sparkVersion'] == v][df['variant'] == variant]
// MAGIC     rect = ax.bar(ind, list(data['runtime']), width, color=colors[i], yerr=list(data['error']), ecolor='black')
// MAGIC     rects.append(rect)
// MAGIC     ind = ind + width
// MAGIC 
// MAGIC   ax.legend( rects, versions, loc='upper left')
// MAGIC 
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df = table("joins_smoothed") \
// MAGIC   .groupBy("queryName", "sparkVersion") \
// MAGIC   .agg( \
// MAGIC     avg("medianRuntimeSeconds").alias("runtime"), \
// MAGIC     expr("stddev(medianRuntimeSeconds)").alias("error")) \
// MAGIC   .withColumn("num", expr("getNum(queryName)")) \
// MAGIC   .withColumn("variant", expr("getVariant(queryName)")) \
// MAGIC   .withColumn("param", expr("getParam(queryName)")) \
// MAGIC   .orderBy("num", "param") \
// MAGIC   .toPandas()
// MAGIC 
// MAGIC def bar_chart(df, graphCol = 'variant', groupCol = 'param', colorCol = 'sparkVersion', heightCol = 'runtime', colors = ['#29B1C1', '#EF6C30', '#21B16B', '#E8302D']):
// MAGIC   graphs = df[graphCol].unique()
// MAGIC   num_graphs = len(graphs)
// MAGIC   rows = int(ceil(num_graphs / 2))
// MAGIC 
// MAGIC   fig, axarr = plt.subplots(num_graphs, figsize=(5, 15))
// MAGIC 
// MAGIC   for idx, graph in enumerate(graphs):
// MAGIC     ax = axarr[idx]
// MAGIC     data = df[df[graphCol] == graph]
// MAGIC 
// MAGIC     ax.set_title(graph)
// MAGIC     groups = list(data[groupCol].unique())
// MAGIC     num_groups = len(groups)
// MAGIC     width = 0.3                  # the width of the bars
// MAGIC     ind = np.arange(num_groups)  # the x locations for the groups
// MAGIC     ax.set_xticklabels(groups)
// MAGIC     ax.set_xticks(np.arange(num_groups) + (width * 1.5))
// MAGIC 
// MAGIC     versions = sorted(list(df[colorCol].unique()))
// MAGIC     rects = []
// MAGIC     for i, v in enumerate(versions):
// MAGIC       data = df[df[colorCol] == v][df[graphCol] == graph]
// MAGIC       rect = ax.bar(ind, list(data[heightCol]), width, color=colors[i], yerr=list(data['error']), ecolor='black')
// MAGIC       rects.append(rect)
// MAGIC       ind = ind + width
// MAGIC 
// MAGIC     ax.legend( rects, versions, loc='upper left')
// MAGIC 
// MAGIC   display(fig)
// MAGIC 
// MAGIC bar_chart(df)

// COMMAND ----------

// MAGIC %md # Query Plan Breakdown

// COMMAND ----------

val queries = tpcds1500.baseData
  .where($"queryName" === getArgument("queryName", ""))
  .groupBy("sparkVersion")
  .agg(first($"result.queryExecution"))
  .orderBy("sparkVersion")

val result = queries.collect().map {
  case Row(version: String, queryExecution: String) =>
    s"""
     |<h1>$version</h1>
     |<pre>$queryExecution</pre>
     """.stripMargin

}.mkString("<br/>")

displayHTML(result)

// COMMAND ----------

