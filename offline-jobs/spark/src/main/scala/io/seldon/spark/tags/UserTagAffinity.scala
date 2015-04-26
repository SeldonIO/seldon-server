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

case class Config(
    local : Boolean = false,
    client : String = "",
    jdbc : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    tagFilterPath : String = null,
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    days : Int = 1,
    tagAttr : String = "",
    minActionsPerUser : Int = 10,
    minTagCount : Int = 4,
    minPcIncrease : Double = 0.1)

class UserTagAffinity(private val sc : SparkContext,config : Config) {

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
  
  def getFilteredActions(minActions : Int,actions : org.apache.spark.rdd.RDD[(Int,Int)]) = {

    actions.groupBy(_._1).filter(_._2.size >= minActions).flatMap(_._2).map(v => (v._2,v._1)) // filter users with no enough actions and transpose to item first
  }
  
  def convertJson(affinity : org.apache.spark.rdd.RDD[(Int,String,Double,Double,Double,Double,Double)]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val userJson = affinity.map{case (user,tag,tagTf,weight,tagPc,tagPcGlobal,pcIncrease) =>
      val json = (("user" -> user ) ~
            ("tag" -> tag ) ~
            ("tagTf" -> tagTf ) ~            
            ("tagPc" -> tagPc ) ~ 
            ("tagPcGlobal" -> tagPcGlobal ) ~             
            ("pcincr" -> pcIncrease ) ~            
            ("weight" -> weight))
       val jsonText = compact(render(json))    
       jsonText
    }
    userJson
  }
  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    println("Loading tags from "+config.jdbc)
    val rddActions = getFilteredActions(config.minActionsPerUser, parseJsonActions(actionsGlob))

    var tagFilterSet = Set[String]()
    if (config.tagFilterPath.nonEmpty)
    {
      tagFilterSet = sc.textFile(config.tagFilterPath).map { x => x.trim().toLowerCase() }.collect().toSet[String]
    }
    val bc_tagFilterSet = sc.broadcast(tagFilterSet)
    // get item tags from db
    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
    
    val numItems = rddItems.count()
    val tagCounts = rddItems.flatMap(_._2.split(",")).map { x => (x.trim().toLowerCase(),1) }.reduceByKey(_ + _).collectAsMap
    val tagPercent = scala.collection.mutable.Map[String,Float]()
    for((t,c) <- tagCounts) tagPercent(t) = c/numItems.toFloat
    println("tagCounts size is "+tagCounts.size)
    
    val rddCombined = rddActions.join(rddItems)

    val rddFeatures = rddCombined.map{ case (item,(user,tags)) => (user,(item,tags))}.groupByKey()
          .mapValues{v =>
        var doc = new StringBuilder()
        var allTags = ListBuffer[String]()
        val tagFilter = bc_tagFilterSet.value
        for ((item,tags) <- v)
        {
           for(tag <- tags.split(","))
           {
             val tagToken = tag.trim().toLowerCase()//.replaceAll("[ :;'\",]", "_")
             if (tagToken.size > 0)
             {
               if (tagFilter.size > 0)
               {
                 if (tagFilter.contains(tagToken))
                  allTags.append(tagToken)
               }
               else
                 allTags.append(tagToken)
             }
           }
        }
        (allTags.mkString(","),v.size)
      }
    
    /*
    val featuresIter = rddFeatures.map(_._2._1.split(",").toSeq)
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(featuresIter)
    val idfModel = new IDF(config.minTermDocFreq).fit(tf)
    val idf = idfModel.idf
    val tfidf = idfModel.transform(tf)
    val fCount = rddFeatures.count()
    val tfidfCount = tfidf.count()
    val tfCount = tf.count()
    println("featuresAg "+fCount+ " tfidf count "+tfidfCount+" tf count "+tfCount)
    val featuresAg = rddFeatures.zip(tfidf).zip(tf)
*/
    //val tfidfSummary: MultivariateStatisticalSummary = Statistics.colStats(tfidf)
    //val avg_tfidf = tfidfSummary.mean
    //val bc_avg_tfidf = sc.broadcast(avg_tfidf)
    val bc_tagPercent = sc.broadcast(tagPercent)
    
    val minTagCount = config.minTagCount
    val minPcIncrease = config.minPcIncrease
    val tagAffinity = rddFeatures.flatMap{case (user,(tags,numDocs)) =>
      var allTags = ListBuffer[(Int,String,Double,Double,Double,Double,Double)]()
      val tagPercent = bc_tagPercent.value
      val tagCounts = tags.split(",").groupBy { l => l }.map(t => (t._1, t._2.length))
      for (tag <- tags.split(",").toSet[String])
      {
        val tag_tf = tagCounts(tag)
        if (tag_tf > minTagCount)
        {
          val tagPc = tag_tf/numDocs.toFloat
          val tagPcGlobal = tagPercent(tag)
          val pc_increase = (tagPc - tagPcGlobal)/tagPcGlobal
          if (pc_increase > minPcIncrease)
          {
            val affinity = pc_increase
            allTags.append((user,tag,tag_tf,affinity,tagPc,tagPcGlobal,pc_increase))
          }
        }
      }
      allTags
    }
   
    val jsonRdd = convertJson(tagAffinity)
    
    val outPath = config.outputPath + "/" + config.client + "/tagaffinity/"+config.startDay
    
    jsonRdd.coalesce(1, false).saveAsTextFile(outPath)
  }
}

 object UserTagAffinity
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[Config]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]("tagFilterPath") valueName("path url") action { (x, c) => c.copy(tagFilterPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[String]('t', "tagAttr") required() valueName("tag attr") action { (x, c) => c.copy(tagAttr = x) } text("attr name in db containing tags")    
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('m', "minActionsPerUser") action { (x, c) =>c.copy(minActionsPerUser = x) } text("min number of actions per user")
    opt[Int]("minTagCount") action { (x, c) =>c.copy(minTagCount = x) } text("min count for tags in user actions")    
    opt[Double]("minPcIncrease") action { (x, c) =>c.copy(minPcIncrease = x) } text("min percentage increase for affinity to be included")        
    }
    
    parser.parse(args, Config()) map { config =>
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
      val cByd = new UserTagAffinity(sc,config)
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