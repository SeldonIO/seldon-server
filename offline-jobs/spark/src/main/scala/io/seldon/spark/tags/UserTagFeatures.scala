package io.seldon.spark.tags

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import io.seldon.spark.SparkUtils
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.feature.IDF

case class UserTagConfig(
    local : Boolean = false,
    client : String = "",
    jdbc : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    tagDay : Int = 1,
    days : Int = 1,
    tagAttr : String = ""
   )

class UserTagFeatures(private val sc : SparkContext,config : UserTagConfig) {

  def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      (user,item)
      }
    
    rdd
  }
  
  def parseTagAffinities(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val user = (json \ "user").extract[Int]
      val tag = (json \ "tag").extract[String]
      val weight = (json \ "weight").extract[Double]
      (user,(tag,weight))
      }
    
    rdd
  }
  
   def getItemTagsFromDb(jdbc : String,attr : String) = 
  {
    val sql = "select * from (SELECT i.item_id,i.client_item_id,unix_timestamp(first_op),CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END" +  
      " tags FROM  items i INNER JOIN item_attr a ON a.name in ('"+attr+"') and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id " +
      " where i.item_id>? and i.item_id<? order by imv.pos) t where not t.tags is null"

    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    sql,
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getString("tags").toLowerCase().trim())
    )
    rdd
  }
  
 
  
 
  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    val rddActions = parseJsonActions(actionsGlob)

    val tagAffinityGlob = config.inputPath + "/" + config.client+"/tagaffinity/"+SparkUtils.getS3UnixGlob(config.tagDay,1)+"/*"
    println("loading tags from "+tagAffinityGlob)
    val tagRdd = parseTagAffinities(tagAffinityGlob).groupByKey
        
    println("Loading tags from "+config.jdbc)
    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
    
    val rddCombined1 = rddActions.join(tagRdd).map{case (user,(item,tagWeights)) => (item,(user,tagWeights))}
    val rddCombined = rddCombined1.join(rddItems)
    
    val vwRdd = rddCombined.map{case (item,((user,tagWeights),itemTags)) =>
      var line = new StringBuilder()
      line  ++= "1 |i "
      for(tag <- itemTags.split(","))
      {
       val tagToken = tag.trim().toLowerCase().replaceAll("[ :;'\",]", "_")
       if (tagToken.size > 0)
       {
        line ++= tagToken
        line ++= " "
        }
       }
       line ++= " |u "
       for ((tag,weight) <- tagWeights)
       {
         val tagToken = tag.trim().toLowerCase().replaceAll("[ :;'\",]", "_")
         if (tagToken.size > 0)
         {
          line ++= tagToken
          line ++= ":"
          line ++= weight.toString()
          line ++= " "
         }
       }
       line.toString()
    }
    
  
    val outPath = config.outputPath + "/" + config.client + "/tagfeatures/"+config.startDay
    
    vwRdd.saveAsTextFile(outPath)
  }
}

 object UserTagFeatures
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[UserTagConfig]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[String]('t', "tagAttr") required() valueName("tag attr") action { (x, c) => c.copy(tagAttr = x) } text("attr name in db containing tags")    
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]("tagDay") action { (x, c) =>c.copy(tagDay = x) } text("tag day in unix time")    
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    }
    
    parser.parse(args, UserTagConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateVWTopicTraining")
      
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
      println(config)
      val cByd = new UserTagFeatures(sc,config)
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