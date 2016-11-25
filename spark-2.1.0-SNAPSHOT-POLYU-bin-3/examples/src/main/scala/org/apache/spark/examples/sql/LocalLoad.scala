/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession

object LocalLoad {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      //  .config("spark.sql.parquet.filterPushdown","false")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      // .config("spark.sql.parquet.enable.bloom.filter", "false")
      //  .config("spark.sql.parquet.bloom.filter.expected.entries", "100000,100")
      //    .config("spark.sql.parquet.bloom.filter.col.name", "rank,value")
//       .config("spark.sql.files.maxPartitionBytes", 1*1024*1024)
//      // .config("spark.sql.codegen.wholeStage", "false")
//      .config("spark.sql.parquet.enable.histogram", "true")
//      .config("spark.sql.parquet.histogram.col.name", "rank,value")
//      .config("spark.sql.parquet.histogram.bound.min", "0,0")
//      .config("spark.sql.parquet.histogram.bound.max", "100,100")
//      .config("spark.sql.parquet.histogram.buckets.num", "10,10")
//        .config("spark.sql.parquet.histogram.optimization", "true")
      .config("spark.eventLog.enabled", "true")
      .master("local[1]")  // test in local mode
      .appName("Test new 2.1.0-SNAPSHOT-POLYU")
      .getOrCreate

    spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 1 * 1024 * 1024)
    // save as parquets
    // val path = saveDataAsParquet(spark)
    // val path: String = "file:///Users/bairan/Documents/ZG4K/LimitIssue/DataSet/_Two_Columns_His"
    val path: String = "file:///Users/bairan/Documents/ZG4K" +
      "/LimitIssue/DataSet/_Two_Columns_N_SmallBlock"

    val parquetFileDF = spark.read.parquet(path)

    // parquetFileDF.printSchema()

    // create temp view
    parquetFileDF.createOrReplaceTempView("SimpleTable")

    val option: Int = args(0).toInt

    val result = option match {
      case 1 => spark.sql("select * from SimpleTable " +
        "where (rank>10 and rank<20) or (value>6 and value<8) or (rank>20 and rank<30)")
      case 2 => spark.sql("select * from SimpleTable " +
        "where value =2 or value=3")

      case 3 => spark.sql("select * from SimpleTable limit 1")
    }
    result.collect()

/*
    val result = spark.sql("select * from SimpleTable " +
      "where (rank>10 and rank<20) or (value>6 and value<8) ")
    result.collect()

    val result1 = spark.sql("select * from SimpleTable " +
      "where (rank>10 and rank<20) or (value>6 and value<8) ")
    result1.collect()

    val result2 = spark.sql("select * from SimpleTable " +
      "where value =2 or value=3")
    result2.collect()

    */
  }

}
