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



case class UniqueUsersConfig(
    local : Boolean = false,
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    minDays : Int = 1
)


class UniqueUsers(private val sc : SparkContext,config : UniqueUsersConfig) {

  
  
  def parseJson(path : String) = {

    val rdd = sc.textFile(path).flatMap{line =>
      
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      try
      {
        val json = parse(line)
        val clientId = (json \ "client_userid").extract[String]
        val userid = (json \ "userid").extract[Long]
        val dateStr = (json \ "timestamp_utc").extract[String]
        val date = formatter.parseDateTime(dateStr)
        val day = (date.getMillis() / 1000) / 86400; 
        
        Seq((userid,(clientId,day)))
      }
      catch
      {
        case ex: MappingException => {println("bad json => "+line)}
        None
      }
      
    }
    
    rdd
  }
  

  def run()
  {
   
     val glob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
     println("loading from "+glob)
     val rddJson = parseJson(glob)
     val minDays = config.minDays
     val users = rddJson.groupByKey().flatMapValues{vals =>
       val days = vals.map(v => v._2).toList.toSet
       val id = vals.map(v => v._1).toList
       if (days.size >= minDays)
         Seq((id(0)))
       else
         None
       }
   
     val outPath = config.outputPath + "/" + config.client + "/unique_users/"+config.startDay
    
     users.saveAsTextFile(outPath)
  }
}

object UniqueUsers {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[UniqueUsersConfig]("UniqueUsers") {
    head("UniqueUsers", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]("client") required() valueName("client") action { (x, c) => c.copy(client = x) } text("client")    
    opt[String]('i', "inputPath") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "outputPath") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('r', "days") valueName("num days") action { (x, c) => c.copy(days = x) } text("num days to process")
    opt[Int]("startDay") valueName("start day") action { (x, c) => c.copy(startDay = x) } text("start day")
    opt[Int]("minDays") valueName("min num days") action { (x, c) => c.copy(minDays = x) } text("min number of days a user must be active")
        }
    
    parser.parse(args, UniqueUsersConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("UNIQUSERS")
      
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
      val cByd = new UniqueUsers(sc,config)
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