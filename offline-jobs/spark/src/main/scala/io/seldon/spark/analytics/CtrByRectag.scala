package io.seldon.spark.analytics

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import io.seldon.spark.SparkUtils
import org.apache.spark.sql.SQLContext



case class CtrByRectagConfig(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    startDate : String = "",
    endDate : String = "",
    influxdb_host : String = "",
    influxdb_user : String = "root",
    influxdb_pass : String = "")

case class ImpressionRectag(consumer: String, year: Int, month: Int, day: Int, click : String, abkey : String, rectag : String)
    
class CtrByRectag(private val sc : SparkContext,config : CtrByRectagConfig) {

  
  
  def parseJson(lines : org.apache.spark.rdd.RDD[String]) = {

    val rdd = lines.flatMap{line =>
      
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

      
      val parts = line.split("\t")
      val date = formatter.parseDateTime(parts(0))
      if (parts(1) == "restapi.ctralg")
      {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      try
      {
        val json = parse(parts(2))
        val consumer = (json \ "consumer").extract[String]
        val click = (json \ "click").extract[String]
        val abkey = (json \ "abkey").extract[String]
        val rectag = (json \ "rectag").extract[String]
        Seq(ImpressionRectag(consumer,date.getYear(),date.getMonthOfYear(),date.getDayOfMonth(),click,abkey,rectag))
      }
      catch
      {
        case ex: MappingException => {println("bad json => "+line)}
        None
      }
      }
      else
        None
    }
    
    rdd
  }
  
  def sendStatsToInfluxDb(data : org.apache.spark.sql.SchemaRDD,iHost : String, iUser : String, iPass : String) = {
    import org.influxdb.InfluxDBFactory
    import java.util.concurrent.TimeUnit
    
    val influxDB = InfluxDBFactory.connect("http://"+iHost+":8086", iUser, iPass);
    
    val rows = data.collect()
    val serie = new org.influxdb.dto.Serie.Builder("impressionsbytag")
            .columns("time", "client", "abkey", "rectag", "impressions", "clicks")
    for(row <- rows)
    {
      val date = new DateTime(row.getInt(3),row.getInt(4),row.getInt(5),0,0)
      val client = row.getString(0)
      val abkey = row.getString(1)
      val rectag = row.getString(2)
      val imps = row.getLong(6)
      val clicks = row.getLong(7)
      serie.values(date.getMillis() : java.lang.Long,client,abkey,rectag,imps : java.lang.Long,clicks : java.lang.Long)
    }
    influxDB.write("stats", TimeUnit.MILLISECONDS, serie.build());
  }
  
  def process(lines : org.apache.spark.rdd.RDD[String],iHost : String, iUser : String, iPass : String) = 
  {
    // sc is an existing SparkContext.
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
       
    val imps = parseJson(lines)
    
    imps.toDF().registerTempTable("imps")
    
   
      val ctr = sqlContext.sql("select t1.consumer,t1.abkey,t1.rectag,t1.year,t1.month,t1.day,impressions,clicks,(clicks/impressions)*100 from (select consumer,abkey,rectag,year,month,day,count(*) as impressions from imps where click='IMP' group by consumer,abkey,rectag,year,month,day) t1 join (select consumer,abkey,rectag,year,month,day,count(*) as clicks from imps where click='CTR' group by consumer,abkey,rectag,year,month,day) t2 on (t1.consumer=t2.consumer and t1.abkey=t2.abkey and t1.rectag=t2.rectag and t1.year=t2.year and t1.month=t2.month and t1.day=t2.day)")
    
      if (iHost.nonEmpty)
      {
        sendStatsToInfluxDb(ctr,iHost,iUser,iPass)
      }
       ctr
  }
  
  def run()
  {
   
    val startDate = DateTime.parse(config.startDate)
    val endDate = DateTime.parse(config.endDate)
    val glob = config.inputPath + "/" + startDate.getYear() + "/" + SparkUtils.getS3UnixGlob(startDate, endDate)+"/*/*"
    println(glob)

    val lines = sc.textFile(glob)
    
    val ctr = process(lines,config.influxdb_host,config.influxdb_user,config.influxdb_pass)

    val outPath = config.outputPath + "/" + config.startDate+"_"+config.endDate

    ctr.rdd.coalesce(1, false).saveAsTextFile(outPath)
  }
}

object CtrByRectag {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[CtrByRectagConfig]("CtrByRectag") {
    head("CtrByRectag", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('d', "start-date") required() valueName("start date") action { (x, c) => c.copy(startDate = x) } text("start date yyyy-mm-dd")
    opt[String]('e', "end-date") required() valueName("end date") action { (x, c) => c.copy(endDate = x) } text("end date yyyy-mm-dd")
    opt[String]("influxdb-host") valueName("influxdb host") action { (x, c) => c.copy(influxdb_host = x) } text("influx db hostname")    
    opt[String]('u', "influxdb-user") valueName("influxdb username") action { (x, c) => c.copy(influxdb_user = x) } text("influx db username")    
    opt[String]('p', "influxdb-pass") valueName("influxdb password") action { (x, c) => c.copy(influxdb_pass = x) } text("influx db password")
    }
    
    parser.parse(args, CtrByRectagConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CTRRECTAG")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      if (config.awsKey.nonEmpty && config.awsSecret.nonEmpty)
      {
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)
      }
      val cByd = new CtrByRectag(sc,config)
      cByd.run()
    }
    finally
    {
      println("Shutting down job")
      sc.stop()
    }
    } getOrElse 
    {
      
    }

    // set up environment

    
  }
}