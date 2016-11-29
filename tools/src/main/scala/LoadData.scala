import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by bairan on 10/11/2016.
  */
object LoadData {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.parquet.enableVectorizedReader","false")
      /*
      .config("spark.sql.parquet.enable.histogram", "true")
      .config("spark.sql.parquet.histogram.col.name", "starttime,formatcallerno,callerno,orgcalledno,calledno,formatcalledno")
      .config("spark.sql.parquet.histogram.bound.min", "1433199100,20500000,20500000,20500000,20500000,20500000")
      .config("spark.sql.parquet.histogram.bound.max", "1433199200,20600000,20600000,20600000,20600000,20600000")
      .config("spark.sql.parquet.histogram.buckets.num", "20,10,10,10,10,10") //H
      .config("spark.sql.parquet.enable.bloom.filter","true")
      .config("spark.sql.parquet.bloom.filter.expected.entries","60000,70000,2,30000,30000") //BF2 -> B
      .config("spark.sql.parquet.bloom.filter.col.name","formatcallerno,callerno,orgcalledno,calledno,formatcalledno")
      */
      .appName("Load Parquet")
      .getOrCreate

    //save as parquets
    val inputSource:String = args(0)
    val outputPath: String = args(1)

    saveDataAsParquet(spark, inputSource, outputPath)

    logger.info("the Loading phase is finished")
  }

  def saveDataAsParquet(spark: SparkSession, input:String, output:String): Unit = {
    import spark.implicits._

    //read data
    val dataDF = spark.sparkContext
      .textFile(input)
      .map(_.split("\\|"))
      .map(attributes => new VoiceCall(attributes))
      .toDF()

    //save as parquet
    dataDF.write.partitionBy("last_msisdn").parquet(output)

    logger.info("SparkSession write task is completed")
  }

}

/**
  * Created by bairan on 10/11/2016.
  */
object TransferUtil {
  def bigIntTransfer(numString: String): BigInt = {
    if ("".equals(numString)) {
      0
    } else {
      BigInt.apply(numString)
    }
  }

  def LongTransfer(numString: String): Long = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toLong
    }
  }

  def intTransfer(numString: String): Int = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toInt
    }
  }

  def patitionKeyTransfer(numString: String): String = {
    if ("".equals(numString)) {
      "NULL"
    } else {
      numString.substring(numString.length - 1)
    }

  }

}


/**
  * Created by bairan on 10/11/2016.
  */
case class VoiceCall (

        cs_refid: String,
        ngn_refid: String,
        starttime: Long,
        millisec: Int,
        service_type: Int,
        callerno: Long,
        callertype: Int,
        callerimsi: String,
        calledno: Long,
        calledtype: Int,
        calledimsi: String,
        alert_time: Long,
        answer_time: Long,
        disconn_time: Long,
        end_time: Long,
        causelst: String,
        protocols: String,
        status: Int,
        reserved1: String,
        reserved2: String,
        reserved3: String,
        reserved4: String,
        signalstorage: String,
        cldnoa: Int,
        cplnoa: Int,
        firfailprot: Long,
        firfailmsg: Int,
        firfailcause: Long,
        callid: String,
        callduration: Long,
        rel_time: Long,
        rlc_time: Long,
        layer1id: Int,
        layer2id: Int,
        layer3id: Int,
        layer4id: Int,
        layer5id: Int,
        layer6id: Int,
        formatcallerno: Long,
        formatcallertype: Int,
        formatcalledno: Long,
        formatcalledtype: Int,
        orgcalledno: Long,
        orgcalledtype: Int,
        orgcallednoa: Int,
        csfbind: Int,
        failtype: String,
        firfailpd: Int,
        firfailside: Int,
        callerrelcgi: String,
        calledrelcgi: String,
        ni: Int,
        opc: Int,
        srcip: Long,
        poolid: Long,
        reserved5: String,
        reserved6: String,
        last_msisdn: String)
{
    def this(attributes: Array[String]) = {

      this(attributes(0), attributes(1), TransferUtil.LongTransfer(attributes(2).trim), TransferUtil.intTransfer(attributes(3).trim),
        TransferUtil.intTransfer(attributes(4).trim), TransferUtil.LongTransfer(attributes(5).trim), TransferUtil.intTransfer(attributes(6).trim),
        attributes(7), TransferUtil.LongTransfer(attributes(8).trim), TransferUtil.intTransfer(attributes(9).trim), attributes(10),
        TransferUtil.LongTransfer(attributes(11).trim), TransferUtil.LongTransfer(attributes(12).trim), TransferUtil.LongTransfer(attributes(13).trim),
        TransferUtil.LongTransfer(attributes(14).trim), attributes(15), attributes(16), TransferUtil.intTransfer(attributes(17).trim),
        attributes(18), attributes(19), attributes(20), attributes(21), attributes(22), TransferUtil.intTransfer(attributes(23).trim),
        TransferUtil.intTransfer(attributes(24).trim), TransferUtil.LongTransfer(attributes(25).trim), TransferUtil.intTransfer(attributes(26).trim),
        TransferUtil.LongTransfer(attributes(27).trim), attributes(28), TransferUtil.LongTransfer(attributes(29).trim), TransferUtil.LongTransfer(attributes(30).trim),
        TransferUtil.LongTransfer(attributes(31).trim), TransferUtil.intTransfer(attributes(32).trim), TransferUtil.intTransfer(attributes(33).trim),
        TransferUtil.intTransfer(attributes(34).trim), TransferUtil.intTransfer(attributes(35).trim), TransferUtil.intTransfer(attributes(36).trim),
        TransferUtil.intTransfer(attributes(37).trim), TransferUtil.LongTransfer(attributes(38).trim), TransferUtil.intTransfer(attributes(39).trim),
        TransferUtil.LongTransfer(attributes(40).trim), TransferUtil.intTransfer(attributes(41).trim), TransferUtil.LongTransfer(attributes(42).trim),
        TransferUtil.intTransfer(attributes(43).trim), TransferUtil.intTransfer(attributes(44).trim), TransferUtil.intTransfer(attributes(45).trim), attributes(46),
        TransferUtil.intTransfer(attributes(47).trim), TransferUtil.intTransfer(attributes(48).trim), attributes(49), attributes(50),
        TransferUtil.intTransfer(attributes(51).trim), TransferUtil.intTransfer(attributes(52).trim), TransferUtil.LongTransfer(attributes(53).trim),
        TransferUtil.LongTransfer(attributes(54).trim), attributes(55), attributes(56), TransferUtil.patitionKeyTransfer(attributes(8))
      )


    }

}

