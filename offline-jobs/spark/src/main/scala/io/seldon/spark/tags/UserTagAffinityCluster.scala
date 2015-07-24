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
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode

case class ClusterTagAffinityConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,
    zkHosts : String = "",
    activate : Boolean = false,
    
    tagFilterPath : String = "",
    jdbc : String = "",
    tagAttr : String = "",
    minActionsPerUser : Int = 10,
    minTagCount : Int = 4,
    minPcIncrease : Double = 0.2)

class UserTagAffinityCluster(private val sc : SparkContext,config : ClusterTagAffinityConfig) {

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
  
  def convertJson(affinity : org.apache.spark.rdd.RDD[(Int,Int,Int,String)]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val userJson = affinity.map{case (user,group,cluster,tags) =>
      val json = (("user" -> user ) ~
            ("group" -> group ) ~
            ("cluster" -> cluster ) ~
            ("tags" -> tags)
            )
       val jsonText = compact(render(json))    
       jsonText
    }
    userJson
  }
  
   def activate(location : String) 
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    import org.apache.curator.utils.EnsurePath
    val curator = new ZkCuratorHandler(config.zkHosts)
    if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
    {
        val zkPath = "/all_clients/"+config.client+"/tagaffinity"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,location.getBytes())
    }
    else
      println("Failed to get zookeeper! Can't activate model")
  }
  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    println("Loading tags from "+config.jdbc)
    val rddActions = getFilteredActions(config.minActionsPerUser, parseJsonActions(actionsGlob))

    // tag, group, cluster within group
    //e.g. 
    //  Liverpool FC,football teams,liverpool
    //  Everton FC,football teams,everton
    val tagFilterDefns = sc.textFile(config.tagFilterPath).map { x => 
        val tagClass = x.split(",") 
        (tagClass(0).trim().toLowerCase(),(tagClass(1).trim().toLowerCase(),tagClass(2).trim().toLowerCase()))
        }.collect().toMap[String,(String,String)]
    for(k <- tagFilterDefns.keys)
    {
      val (group,cluster) = tagFilterDefns(k)
      println(k+","+tagFilterDefns(k))
    }
    // create idx for clusters and groups
    val groupIdx = tagFilterDefns.map{case (tag,(group,cluster)) => group}.toSet.zipWithIndex.toMap
    val clusterIdx = tagFilterDefns.map{case (tag,(group,cluster)) => cluster}.toSet.zipWithIndex.toMap
    val clusterToGroupIdx = tagFilterDefns.map{case (tag,(group,cluster)) => (clusterIdx(cluster),groupIdx(group))}.toMap
    
    // create broadcast data
    val bc_groupIdx = sc.broadcast(groupIdx)
    val bc_clusterIdx = sc.broadcast(clusterIdx)
    val bc_clusterToGroupIdx = sc.broadcast(clusterToGroupIdx)
    val bc_tagFilterDefns = sc.broadcast(tagFilterDefns)

    // get item tags from db
    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
    
    // Calculate for each tag the percentage of articles in which it appears
    
    val rddCombined = rddActions.join(rddItems)
    
    val numActions = rddCombined.count()
    val tagCounts = rddCombined.flatMap(_._2._2.split(",")).map { x => (x.trim().toLowerCase(),1) }.reduceByKey(_ + _).collectAsMap
    val tagPercent = scala.collection.mutable.Map[String,Float]()
    for((t,c) <- tagCounts) tagPercent(t) = c/numActions.toFloat
    println("tagCounts size is "+tagCounts.size)
    
    // extract tags and filter to desired tags
    val rddFeatures = rddCombined.map{ case (item,(user,tags)) => (user,(item,tags))}.groupByKey()
          .mapValues{v =>
        var doc = new StringBuilder()
        var allTags = ListBuffer[String]()
        val tagFilter = bc_tagFilterDefns.value
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
   
    val bc_tagPercent = sc.broadcast(tagPercent)
    
    val minTagCount = config.minTagCount
    val minPcIncrease = config.minPcIncrease
    val minUserActions = config.minActionsPerUser
    val tagAffinity = rddFeatures.flatMap{case (user,(tags,numDocs)) =>
      var allTags = ListBuffer[(Int,Int,Int,String)]()
      var clusterToCount = collection.mutable.Map[Int, Int]().withDefaultValue(0)
      var tagsFound = collection.mutable.Map[Int, String]().withDefaultValue("")
      if (numDocs >= minUserActions)
      {
        val tagPercent = bc_tagPercent.value
        val tagDefns = bc_tagFilterDefns.value
        val clusterIdx = bc_clusterIdx.value
        val groupIdx = bc_groupIdx.value
        val clusterToGroupIdx = bc_clusterToGroupIdx.value
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
              val (group,cluster) = tagDefns(tag)
              val groupId = groupIdx(group)
              val clusterId = clusterIdx(cluster)
              clusterToCount(clusterId) += 1
              tagsFound(clusterId) += (tag+ ",")
            }
          }
        }
      }
      for(k <- clusterToCount.keys)
      {
        allTags.append((user,clusterToGroupIdx(k),k,tagsFound(k)))
      }
      allTags
    }
   
    val jsonRdd = convertJson(tagAffinity)
    
    val outPath = config.outputPath + "/" + config.client + "/tagcluster/"+config.startDay
    
    jsonRdd.coalesce(1, false).saveAsTextFile(outPath)
    
    /* Not Needed?
    val clusterToGroupCSV = clusterToGroupIdx.map{case (cluster,group) => cluster.toString()+","+group.toString()}
    FileUtils.outputModelToFile(clusterToGroupCSV.toArray, outPath, DataSourceMode.fromString(outPath), "clusterToGroup.csv")
    val clusterIdxCSV = clusterIdx.map{case (cluster,idx) => cluster +","+idx.toString()}
    FileUtils.outputModelToFile(clusterIdxCSV.toArray, outPath, DataSourceMode.fromString(outPath), "cluster.csv")
    val groupIdxCSV = groupIdx.map{case (group,idx) => group+","+idx.toString()}
    FileUtils.outputModelToFile(groupIdxCSV.toArray, outPath, DataSourceMode.fromString(outPath), "group.csv")
    */
    val tagFilterDefnsCSV = tagFilterDefns.map{case (tag,(groupName,clusterName)) =>
      val groupId = groupIdx(groupName)
      val clusterId = clusterIdx(clusterName)
      tag+","+groupId.toString()+","+clusterId.toString()
    }
    FileUtils.outputModelToFile(tagFilterDefnsCSV.toArray, outPath, DataSourceMode.fromString(outPath), "tags.csv")
     if (config.activate)
       activate(outPath)
  }
}

 object UserTagAffinityCluster
{
    def updateConf(config : ClusterTagAffinityConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/tagcluster"
       if (curator.getCurator.checkExists().forPath(path) != null)
       {
         val bytes = curator.getCurator.getData().forPath(path)
         val j = new String(bytes,"UTF-8")
         println("Confguration from zookeeper -> "+j)
         import org.json4s._
         import org.json4s.jackson.JsonMethods._
         implicit val formats = DefaultFormats
         val json = parse(j)
         import org.json4s.JsonDSL._
         import org.json4s.jackson.Serialization.write
         type DslConversion = ClusterTagAffinityConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[ClusterTagAffinityConfig] // extract case class from merged json
         c
       }
       else 
       {
           println("Warning: using default configuaration - path["+path+"] not found!");
           c
       }
     }
     else 
     {
       println("Warning: using default configuration - no zkHost!");
       c
     }
  }
  
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    var c = new ClusterTagAffinityConfig()
    val parser = new scopt.OptionParser[Unit]("UserTagAffinityCluster") {
    head("UserTagAffinityCluster", "1.0")
       opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")

        opt[String]('j', "jdbc") valueName("<JDBC URL>") foreach { x => c = c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
        opt[Int]('m', "minActionsPerUser") foreach { x => c = c.copy(minActionsPerUser = x) } text("min number of actions per user")
        opt[String]("tagFilterPath") valueName("path url") foreach { x => c = c.copy(tagFilterPath = x) } text("tag filter path")        
        opt[String]("tagAttr") valueName("tag attr") foreach { x => c = c.copy(tagAttr = x) } text("db attribute name containing tags")                
        opt[Int]("minTagCount") foreach { x => c = c.copy(minTagCount = x) } text("min count for tags in user actions")    
        opt[Double]("minPcIncrease") foreach { x => c = c.copy(minPcIncrease = x) } text("min percentage increase for affinity to be included")        

    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("UserTagAffinityCluster")

      if (c.local)
        conf.setMaster("local")
 //       .set("spark.akka.frameSize", "300")

      val sc = new SparkContext(conf)
      try
      {
        sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if (c.awsKey.nonEmpty && c.awsSecret.nonEmpty)
        {
         sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", c.awsKey)
         sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", c.awsSecret)
        }
        println(c)
        val cu = new UserTagAffinityCluster(sc,c)
        cu.run()
      }
      finally
      {
        println("Shutting down job")
        sc.stop()
      }
   } 
   else 
   {
      
   }
  }
}
