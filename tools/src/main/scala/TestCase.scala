/**
  * Created by bairan on 29/11/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io._

object TestCase {
  def main(args: Array[String]) {

    /**
      * args(0): version number to note
      * args(1): input parquet path
      * args(2): output runtime file path
      *
      */

    val spark: SparkSession = SparkSession.builder
      //  .config("spark.sql.parquet.filterPushdown","false")
      .config("spark.sql.parquet.enableVectorizedReader","false")
      .appName("Test Version_"+args(0)+ "_benchmarks")
      .getOrCreate


    // sleep 10s to wait for executor
    Thread.sleep(10000)

    val pw = new PrintWriter(new File(args(2) + "/Version_" + args(0)+ "_Results"))
    val Path: String = args(1)
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
    val result = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200")
    var str:String ="******* "+ "Warming up is finished" + "*******" + result.collect().length +"\n"
    var endtime:Long = System.nanoTime()
    var str2:String= "++++++> Warming up Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    /**
      * Below are the Six Benchmarks
      */

    starttime = System.nanoTime()
    val result1 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200  AND last_msisdn = '0' limit 5000")
    var result_size:String ="Result size: "+result1.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #1 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)


    starttime = System.nanoTime()
    val result2 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) AND last_msisdn = '0' limit 5000")
    result_size="Result size: "+result2.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #2 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    starttime = System.nanoTime()
    val result3 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200 limit 5000")
    result_size="Result size: "+result3.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #3 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    starttime = System.nanoTime()
    val result4 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND " +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) limit 5000")
    result_size="Result size: "+result4.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #4 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    starttime = System.nanoTime()
    val result5 = spark.sql("select * from VOICE_CALL where starttime > 1433199150 AND" +
      "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
      " AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = '')) limit 5000")
    result_size="Result size: "+result5.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #5 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    starttime = System.nanoTime()
    val result6 = spark.sql("select cs_refid,ngn_refid,starttime,millisec,service_type from VOICE_CALL where starttime > 1433199150 AND" +
      "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
      " AND starttime < 1433199200 limit 5000")
    result_size="Result size: "+result6.collect().length+"\n"
    endtime = System.nanoTime()
    str="======> "+result_size
    str2="++++++> Query #6 Run time: " + (endtime-starttime)/1000000000.0+"\n"
    pw.write(str+str2)

    pw.close()

    logger.info("suicide")
    spark.sparkContext.stop()

  }
}

