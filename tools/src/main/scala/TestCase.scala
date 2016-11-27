/**
  * Created by lizbai on 8/9/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TestCase {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.parquet.enableVectorizedReader","false")
      .appName("[Beta1.2] "+args(0)+" benchmarks") //no bf
      .getOrCreate


    val Path: String = "hdfs://dbg11:8020/user/root/test/voice_call_parquet_"+ args(0)
    //read parquet file
    val parquetFileDF = spark.read.parquet(Path)

    //parquetFileDF.printSchema()

    //create temp view
    parquetFileDF.createOrReplaceTempView("VOICE_CALL")
    val logger = LoggerFactory.getLogger(getClass)

    /**
      * Warm up query
      */
    var starttime:Long = System.nanoTime()
    val result = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND" +
      "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
      "AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = '')) limit 1")
    logger.info("******* "+ "Warming up is finished" + "*******" + result.collect().length)
    var endtime:Long = System.nanoTime()
    logger.info("++++++> Warming up Run time: " + (endtime-starttime))

    /**
      * Below are the Six Benchmarks
      */
    starttime = System.nanoTime()
    val result1 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND last_msisdn = '0' limit 5000")
    var result_size:String ="Result size: "+result1.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #1 Run time: " + (endtime-starttime))

    starttime = System.nanoTime()
    val result2 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) AND last_msisdn = '0' limit 5000")
    result_size="Result size: "+result2.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #2 Run time: " + (endtime-starttime))

    starttime = System.nanoTime()
    val result3 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200 limit 5000")
    result_size="Result size: "+result3.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #3 Run time: " + (endtime-starttime))

    starttime = System.nanoTime()
    val result4 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) limit 5000")
    result_size="Result size: "+result4.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #4 Run time: " + (endtime-starttime))

    starttime = System.nanoTime()
    val result5 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND" +
        "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
        "AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = '')) limit 5000")
    result_size="Result size: "+result5.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #5 Run time: " + (endtime-starttime))

    starttime = System.nanoTime()
    val result6 = spark.sql("select cs_refid,ngn_refid,starttime,millisec,service_type from VOICE_CALL where starttime > 1433199150 AND" +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200 limit 5000")
    result_size="Result size: "+result6.collect().length
    endtime = System.nanoTime()
    logger.info("======> "+result_size)
    logger.info("++++++> Query #6 Run time: " + (endtime-starttime))

    logger.info("suicide")
    spark.sparkContext.stop()

  }
}
