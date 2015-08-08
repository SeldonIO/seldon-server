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



case class RecentAPICallsConfig(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    startDate : String = "",
    endDate : String = "",
    influxdb_host : String = "",
    influxdb_user : String = "root",
    influxdb_pass : String = "",
    filterUsersFile : String = "")


class RecentAPICalls(private val sc : SparkContext,config : RecentAPICallsConfig) {

  
  
  def parseJson(lines : org.apache.spark.rdd.RDD[String]) = {

    val rdd = lines.flatMap{line =>
      
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      
      val parts = line.split("\t")
      val date = formatter.parseDateTime(parts(0))
      if (parts(1) == "restapi.test")
      {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      try
      {
        val json = parse(parts(2))
        val path = (json \ "path").extract[String]
        val query = (json \ "query").extract[String]
        // remove user from query string
        import com.netaporter.uri.Uri.parse
        val uriStr = "http:api.rummblelabs.com"+path+"?"+query
        val uri = parse(uriStr)
        uri.filterQueryNames(_ > "user")
        Seq(uri.toString)
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
    
  def run()
  {
   
    val startDate = DateTime.parse(config.startDate)
    val endDate = DateTime.parse(config.endDate)
    val glob = config.inputPath + "/" + startDate.getYear() + "/" + SparkUtils.getS3UnixGlob(startDate, endDate)+"/*/*"
    println(glob)

    val lines = sc.textFile(glob)
    
    val uniqueUrls = parseJson(lines).distinct()
    
    val outPath = config.outputPath + "/" + config.startDate+"_"+config.endDate

    uniqueUrls.coalesce(1, false).saveAsTextFile(outPath)
  }
}

object RecentAPICalls {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[RecentAPICallsConfig]("RecentAPICalls") {
    head("RecentAPICalls", "1.x")
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
    opt[String]("filterUsersFile") valueName("filter users file") action { (x, c) => c.copy(filterUsersFile = x) } text("filter users file")    
    }
    
    parser.parse(args, RecentAPICallsConfig()) map { config =>
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
      val cByd = new RecentAPICalls(sc,config)
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