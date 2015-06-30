package io.seldon.spark.transactions


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import io.seldon.spark.SparkUtils
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import scala.util.Random

case class BasketAnalysisConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",
    
    maxIntraSessionGapSecs : Int =  -1,
    minActionsPerUser : Int = 0,
    maxActionsPerUser : Int = 100000,
    addToBasketType : Int = 1,
    removeFromBasketType : Int = 2)

class BasketAnalysis(private val sc : SparkContext,config : BasketAnalysisConfig) {

  def parseJsonActions(path : String,addToBasket : Int, removeFromBasket : Int) = {
    
    val rdd = sc.textFile(path).flatMap{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      val dateUtc = (json \ "timestamp_utc").extract[String]
      val actionType = (json \ "type").extract[Int]

      if (actionType == addToBasket || actionType == removeFromBasket)
      {
        val date1 = org.joda.time.format.ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(dateUtc)
        Seq((user,(item,actionType,date1.getMillis())))
      }
      else
        None
    }
    
    rdd
  }
  

  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    val rddActions = parseJsonActions(actionsGlob,config.addToBasketType,config.removeFromBasketType)
    
    val minNumActions = config.minActionsPerUser
    val maxNumActions = config.maxActionsPerUser
    val maxGapMsecs = config.maxIntraSessionGapSecs * 1000
    val addToBasketType = config.addToBasketType
    val removeFromBasketType = config.removeFromBasketType
      // create feature for current item and the user history of items viewed
    val rddFeatures = rddActions.groupByKey().filter(_._2.size >= minNumActions).filter(_._2.size <= maxNumActions).flatMapValues{v =>
        val buf = new ListBuffer[String]()
        val basket = scala.collection.mutable.Set[Int]()
        val sorted = v.toArray.sortBy(_._3)
        var lastTime : Long = 0
        var timeSecs : Long = 0
        for ((item,actionType,t) <- sorted)
        {
          println("looking at ",item,"type=",actionType," time=",t)
          if (lastTime > 0)
          {
            val gap = (t - lastTime)
            if (maxGapMsecs > -1 && gap > maxGapMsecs && actionType == addToBasketType)
            {
              val lineStr = basket.mkString("", " ", "")
              if (lineStr.length() > 0)
                buf.append(lineStr)
              basket.clear()
            }
          }
          if (actionType == addToBasketType)
            basket.add(item)
          else
            basket.remove(item)
          lastTime = t
         } 
         val lineStr = basket.mkString("", " ", "")
         if (lineStr.length() > 0)
           buf.append(lineStr)
         buf 
      }.values
      val outPath = config.outputPath + "/" + config.client + "/basketanalysis/"+config.startDay
      rddFeatures.saveAsTextFile(outPath)
  }
}

 object BasketAnalysis
{
   
  def updateConf(config : BasketAnalysisConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/basketanalysis"
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
         type DslConversion = BasketAnalysisConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[BasketAnalysisConfig] // extract case class from merged json
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

    var c = new BasketAnalysisConfig()
    val parser = new scopt.OptionParser[Unit]("BasketAnalysis") {
    head("BasketAnalysis", "1.0")
       opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        

        opt[Int]("minActionsPerUser") foreach { x => c = c.copy(minActionsPerUser = x) } text("min number of actions per user")
        opt[Int]("maxActionsPerUser") foreach { x => c = c.copy(maxActionsPerUser = x) } text("max number of actions per user")    
        opt[Int]("maxIntraSessionGapSecs") foreach { x => c = c.copy(maxIntraSessionGapSecs = x) } text("max number of secs before assume session over")        
        opt[Int]("addToBasketType") foreach { x => c = c.copy(addToBasketType = x) } text("add to basket action type")                
        opt[Int]("removeFromBasketType") foreach { x => c = c.copy(removeFromBasketType = x) } text("remove from basket type")                        
    }
    
     if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("BasketAnalysis")

      if (c.local)
        conf.setMaster("local")

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
        val si = new BasketAnalysis(sc,c)
        si.run()
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