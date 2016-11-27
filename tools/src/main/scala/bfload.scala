/**
  * Created by lizbai on 16/10/16.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object BFDataLoad {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val spark_builder = SparkSession.builder
      .appName("Load raw Parquet")

    val inputSource:String = args(0)
    val outputPath: String = args(1)

    if ( args.size > 3 ){
      spark_builder
        .config("spark.sql.parquet.enable.bloom.filter","true")
        .config("spark.sql.parquet.bloom.filter.expected.entries","4225088,4225088,20000,155468,82200")
        .config("spark.sql.parquet.bloom.filter.col.name","MSISDN,IMSI,IMEI,SID,BEGIN_TIME")
        .appName("Load raw Parquet with BF")
    }

    val spark: SparkSession = spark_builder.getOrCreate()

    //save as parquets
    saveDataAsParquet(spark, inputSource, outputPath)

    logger.info("the Loading phase is finished")
  }

  def saveDataAsParquet(spark: SparkSession, input: String, output: String): Unit = {
    import spark.implicits._

    //read data
    val dataDF = spark.sparkContext
      .textFile(input)
      .map(_.split("\\|"))
      .map(attributes => new Streaming(attributes))
      .toDF()

    //save as parquet
    dataDF.write.partitionBy("last_msisdn").parquet(output)

    logger.info("SparkSession write task is completed")

  }
}

/**
  * Created by lizbai on 15/10/16.
  */
case class Streaming (
                       SID:          Long,
                       PROBEID:      Long,
                       IntERFACEID:  Int,
                       GROUPID:      Int,
                       GGSN_ID:      Long,
                       SGSN_ID:      Long,
                       SESSION_INDICATOR:          Int,
                       BEGIN_TIME:   Long,
                       BEGIN_TIME_MSEL:            Int,
                       END_TIME:     Long,
                       END_TIME_MSEL:Int,
                       PROT_CATEGORY:Int,
                       PROT_TYPE:    Int,
                       L7_CARRIER_PROT:            Int,
                       MSISDN:       Long,
                       IMSI:         Long,
                       IMEI:         Long,
                       ENCRYPT_VERSION:            Int,
                       ROAMING_TYPE: Int,
                       ROAM_DIRECTION:             Int,
                       MS_IP:        String,
                       SERVER_IP:    String,
                       MS_PORT:      Int,
                       SERVER_PORT:  Int,
                       APN:          String,
                       SGSN_SIG_IP:  String,
                       GGSN_SIG_IP:  String,
                       SGSN_USER_IP: String,
                       GGSN_USER_IP: String,
                       MCC:          String,
                       MNC:          String,
                       RAT:          Int,
                       LAC:          String,
                       RAC:          String,
                       SAC:          String,
                       CI:           String,
                       BROWSER_TYPE: Int,
                       L4_UL_THROUGHPUT:           Long,
                       L4_DW_THROUGHPUT:           Long,
                       L4_UL_GOODPUT:Long,
                       L4_DW_GOODPUT:Long,
                       NETWORK_UL_TRAFFIC:         Long,
                       NETWORK_DL_TRAFFIC:         Long,
                       L4_UL_PACKETS:Long,
                       L4_DW_PACKETS:Long,
                       TCP_CONN_STATES:            Int,
                       TCP_STATES:   Int,
                       TCP_RTT:      Long,
                       TCP_UL_OUTOFSEQU:           Long,
                       TCP_DW_OUTOFSEQU:           Long,
                       TCP_UL_RETRANS:             Long,
                       TCP_DW_RETRANS:             Long,
                       TCP_WIN_SIZE: Int,
                       TCP_MSS:      Int,
                       TCP_CONN_TIMES:             Int,
                       TCP_CONN_2_FAILED_TIMES:    Int,
                       TCP_CONN_3_FAILED_TIMES:    Int,
                       HOST:         String,
                       STREAMING_URL:String,
                       GET_STREAMING_FAILED_CODE:  Int,
                       GET_STREAMING_FLAG:         Int,
                       GET_STREAMING_DELAY:        Long,
                       GET_NUM:      Int,
                       GET_SUCCEED_NUM:            Int,
                       GET_RETRANS_NUM:            Int,
                       GET_TIMEOUT_NUM:            Int,
                       GET_MAX_UL_SIZE:            Long,
                       GET_MIN_UL_SIZE:            Long,
                       GET_MAX_DL_SIZE:            Long,
                       GET_MIN_DL_SIZE:            Long,
                       IntBUFFER_FST_FLAG:         Int,
                       IntBUFFER_FULL_FLAG:        Int,
                       IntBUFFER_FULL_DELAY:       Long,
                       STALL_NUM:    Int,
                       STALL_DURATION:             Long,
                       STREAMING_DW_PACKETS:       Long,
                       STREAMING_DOWNLOAD_DELAY:   Long,
                       PLAY_DURATION:Long,
                       STREAMING_QUALITY:          Int,
                       VIDEO_DATA_RATE:            Long,
                       VIDEO_FRAME_RATE:           Int,
                       VIDEO_CODEC_ID:             String,
                       VIDEO_WIDTH:  Int,
                       VIDEO_HEIGHT: Int,
                       AUDIO_DATA_RATE:            Long,
                       AUDIO_CODEC_ID:             String,
                       STREAMING_FILESIZE:         Long,
                       STREAMING_DURATIOIN:        Long,
                       MEDIA_FILE_TYPE:            Int,
                       PLAY_STATE:   Int,
                       STREAMING_FLAG:             Int,
                       TCP_STATUS_INDICATOR:       Int,
                       DISCONNECTION_FLAG:         Int,
                       FAILURE_CODE: Int,
                       FLAG:         Int,
                       FLOW_SAMPLE_RATIO:          Int,
                       last_msisdn: String
                     ){
  def this(at: Array[String]) = {
    this(
      TransferUtil.LongTransfer(at(0).trim),
      TransferUtil.LongTransfer(at(1).trim),
      TransferUtil.IntTransfer(at(2).trim),
      TransferUtil.IntTransfer(at(3).trim),
      TransferUtil.LongTransfer(at(4).trim),
      TransferUtil.LongTransfer(at(5).trim),
      TransferUtil.IntTransfer(at(6).trim),
      TransferUtil.LongTransfer(at(7).trim),
      TransferUtil.IntTransfer(at(8).trim),
      TransferUtil.LongTransfer(at(9).trim),
      TransferUtil.IntTransfer(at(10).trim),
      TransferUtil.IntTransfer(at(11).trim),
      TransferUtil.IntTransfer(at(12).trim),
      TransferUtil.IntTransfer(at(13).trim),
      TransferUtil.LongTransfer(at(14).trim),
      TransferUtil.LongTransfer(at(15).trim),
      TransferUtil.LongTransfer(at(16).trim),
      TransferUtil.IntTransfer(at(17).trim),
      TransferUtil.IntTransfer(at(18).trim),
      TransferUtil.IntTransfer(at(19).trim),
      at(20),
      at(21),
      TransferUtil.IntTransfer(at(22).trim),
      TransferUtil.IntTransfer(at(23).trim),
      at(24),
      at(25),
      at(26),
      at(27),
      at(28),
      at(29),
      at(30),
      TransferUtil.IntTransfer(at(31).trim),
      at(32),
      at(33),
      at(34),
      at(35),
      TransferUtil.IntTransfer(at(36).trim),
      TransferUtil.LongTransfer(at(37).trim),
      TransferUtil.LongTransfer(at(38).trim),
      TransferUtil.LongTransfer(at(39).trim),
      TransferUtil.LongTransfer(at(40).trim),
      TransferUtil.LongTransfer(at(41).trim),
      TransferUtil.LongTransfer(at(42).trim),
      TransferUtil.LongTransfer(at(43).trim),
      TransferUtil.LongTransfer(at(44).trim),
      TransferUtil.IntTransfer(at(45).trim),
      TransferUtil.IntTransfer(at(46).trim),
      TransferUtil.LongTransfer(at(47).trim),
      TransferUtil.LongTransfer(at(48).trim),
      TransferUtil.LongTransfer(at(49).trim),
      TransferUtil.LongTransfer(at(50).trim),
      TransferUtil.LongTransfer(at(51).trim),
      TransferUtil.IntTransfer(at(52).trim),
      TransferUtil.IntTransfer(at(53).trim),
      TransferUtil.IntTransfer(at(54).trim),
      TransferUtil.IntTransfer(at(55).trim),
      TransferUtil.IntTransfer(at(56).trim),
      at(57),
      at(58),
      TransferUtil.IntTransfer(at(59).trim),
      TransferUtil.IntTransfer(at(60).trim),
      TransferUtil.LongTransfer(at(61).trim),
      TransferUtil.IntTransfer(at(62).trim),
      TransferUtil.IntTransfer(at(63).trim),
      TransferUtil.IntTransfer(at(64).trim),
      TransferUtil.IntTransfer(at(65).trim),
      TransferUtil.LongTransfer(at(66).trim),
      TransferUtil.LongTransfer(at(67).trim),
      TransferUtil.LongTransfer(at(68).trim),
      TransferUtil.LongTransfer(at(69).trim),
      TransferUtil.IntTransfer(at(70).trim),
      TransferUtil.IntTransfer(at(71).trim),
      TransferUtil.LongTransfer(at(72).trim),
      TransferUtil.IntTransfer(at(73).trim),
      TransferUtil.LongTransfer(at(74).trim),
      TransferUtil.LongTransfer(at(75).trim),
      TransferUtil.LongTransfer(at(76).trim),
      TransferUtil.LongTransfer(at(77).trim),
      TransferUtil.IntTransfer(at(78).trim),
      TransferUtil.LongTransfer(at(79).trim),
      TransferUtil.IntTransfer(at(80).trim),
      at(81),
      TransferUtil.IntTransfer(at(82).trim),
      TransferUtil.IntTransfer(at(83).trim),
      TransferUtil.LongTransfer(at(84).trim),
      at(85),
      TransferUtil.LongTransfer(at(86).trim),
      TransferUtil.LongTransfer(at(87).trim),
      TransferUtil.IntTransfer(at(88).trim),
      TransferUtil.IntTransfer(at(89).trim),
      TransferUtil.IntTransfer(at(90).trim),
      TransferUtil.IntTransfer(at(91).trim),
      TransferUtil.IntTransfer(at(92).trim),
      TransferUtil.IntTransfer(at(93).trim),
      TransferUtil.IntTransfer(at(94).trim),
      TransferUtil.IntTransfer(at(95).trim),
      TransferUtil.patitionKeyTransfer(at(14).trim)
    )
  }
}



/**
  * Created by lizbai on 15/10/16.
  */
object TransferUtil {
  def LongTransfer(numString: String): Long = {
    if ("".equals(numString)) {
      0
    } else {
      numString.toLong
    }
  }

  def IntTransfer(numString: String): Int = {
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


  def BigIntTransfer(numString: String): BigInt = {
    if ("".equals(numString)) {
      0
    } else {
      BigInt.apply(numString)
    }
  }
}