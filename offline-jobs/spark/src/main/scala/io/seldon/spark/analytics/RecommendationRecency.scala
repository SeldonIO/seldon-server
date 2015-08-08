package io.seldon.spark.analytics


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import io.seldon.spark.SparkUtils
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.ListBuffer
import java.sql.ResultSet

case class RecommendationRecencyConfig(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    startDate : String = "",
    endDate : String = "",
    client : String = "",
    jdbc : String = "",
    publishAttr : String = ""
)

case class RecencyImpression(year: Int, month: Int, day: Int, hour : Int, recencySecs : Long)
    
class RecommendationRecency(private val sc : SparkContext,config : RecommendationRecencyConfig) {

    def getItemDateFromDb(jdbc : String,attr : String) = 
  {
    val sql = "select * from (SELECT i.item_id,i.client_item_id,unix_timestamp(first_op),CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END" +  
      " dt FROM  items i INNER JOIN item_attr a ON a.name in ('"+attr+"') and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id " +
      " where i.item_id>? and i.item_id<? order by imv.pos) t where not t.dt is null"

    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    sql,
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getTimestamp("dt").getTime)
    )
    rdd
  }
    
  
  
  def parseJson(lines : org.apache.spark.rdd.RDD[String],client : String) = {
    
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
        if (client.equals(consumer) && click.equals("IMP"))
        {
          val recs = (json \ "recommendations").extract[String]
          var recList = ListBuffer[(Int,(Long,Int,Int,Int,Int))]()
          for (rec <- recs.split(":"))
          {
            recList.append((rec.toInt,(date.getMillis,date.getYear,date.getMonthOfYear,date.getDayOfMonth,date.getHourOfDay)))
          }
          recList
        }
        else
          None
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
  
  def process(rdd : org.apache.spark.rdd.RDD[RecencyImpression]) = 
  {
    // sc is an existing SparkContext.
    val sqlContext = new HiveContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    rdd.toDF().registerTempTable("recency")
    
    val averages = sqlContext.sql("select year,month,day,hour,percentile(recencySecs,array(0.25,0.5,0.75,0.9,0.99)) as avgrecency from recency group by year,month,day,hour")

    averages
  }
  
  def computePercentile(data: org.apache.spark.rdd.RDD[Long], tile: Double): Double = {
   // NIST method; data to be sorted in ascending order
   val r = data.sortBy(x => x)
   val c = r.count()
   if (c == 1) r.first()
   else {
     val n = (tile / 100d) * (c + 1d)
     val k = math.floor(n).toLong
     val d = n - k
     if (k <= 0) r.first()
     else {
       val index = r.zipWithIndex().map(_.swap)
       val last = c
       if (k >= c) {
         index.lookup(last - 1).head
       } else {
         index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
       }
     }
   }
 }
 
  def run()
  {
   
    val startDate = DateTime.parse(config.startDate)
    val endDate = DateTime.parse(config.endDate)
    val glob = config.inputPath + "/" + startDate.getYear() + "/" + SparkUtils.getS3UnixGlob(startDate, endDate)+"/*/*"
    println(glob)

    val publishDate = getItemDateFromDb(config.jdbc, config.publishAttr)
    val lines = parseJson(sc.textFile(glob),config.client)
    
    val joined = lines.join(publishDate)
    
    val delta = joined.map{case (item,((tsImps,year,month,day,hour),tsPublish)) =>
      //println("item "+item.toString()+"tsImps: "+tsImps.toString()+" tsPublish "+tsPublish.toString())
      val gapSecs = (tsImps-tsPublish)/1000
      RecencyImpression(year,month,day,hour,gapSecs)}
   
    
        
    val outPath = config.outputPath + "/" + config.client+ "/recommendation_recency/" + config.startDate+"_"+config.endDate

    val recency = process(delta)
    
   recency.rdd.coalesce(1, false).saveAsTextFile(outPath)
  }
}

object RecommendationRecency {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[RecommendationRecencyConfig]("RecommendationRecency") {
    head("RecommendationRecency", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "inputPath") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "outputPath") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('d', "startDate") required() valueName("start date") action { (x, c) => c.copy(startDate = x) } text("start date yyyy-mm-dd")
    opt[String]('e', "endDate") required() valueName("end date") action { (x, c) => c.copy(endDate = x) } text("end date yyyy-mm-dd")
    opt[String]("client") required() valueName("client") action { (x, c) => c.copy(client = x) } text("client name")
    opt[String]("jdbc") required() valueName("jdbc") action { (x, c) => c.copy(jdbc = x) } text("JDBC connection string to db")
    opt[String]("publishAttr") required() valueName("publish attr") action { (x, c) => c.copy(publishAttr = x) } text("publish date attribute name")    
    }
    
    parser.parse(args, RecommendationRecencyConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("RecRecency")
      
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
      val cByd = new RecommendationRecency(sc,config)
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