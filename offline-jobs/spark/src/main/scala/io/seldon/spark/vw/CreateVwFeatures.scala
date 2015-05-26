package io.seldon.spark.vw

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import io.seldon.spark.zookeeper.ZkCuratorHandler
import org.joda.time.format.DateTimeFormat
import io.seldon.spark.SparkUtils
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode
import scala.collection.mutable.ArrayBuffer


case class VwFeatureConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",

    excludedFeatures : String = "client,timestamp",
    targetFeature : String = "userid",
    oaa : Boolean = true
   )

class CreateVwFeatures(private val sc : SparkContext,config : VwFeatureConfig) {
 
   def parseJsonFeatures(path : String,targetFeature : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      val json = parse(line)
      val m = json.extract[Map[String,String]]
     
      val targetVal = (json \ targetFeature).extract[String]

      (targetVal,m)
      }
    
    rdd
  }
   
   
   def createVwLine(rdd : org.apache.spark.rdd.RDD[(String,Map[String,String])],targets : scala.collection.Map[String,Long],excluded : Set[String],oaa : Boolean) : org.apache.spark.rdd.RDD[String] =
   {
     val bcTargets = sc.broadcast(targets)
     val vwLines = rdd.map{case (target,features) =>
       
       var line = new StringBuilder()
       val tMap = bcTargets.value
       if (oaa)
       {
        line ++= (tMap(target)+1).toString()  
       }
       else
       {
         val targetId = tMap(target)
         if (targetId == 1)
           line ++= "1"
         else
           line ++= "-1"
       }
       
       line ++= " |"
       
       for((k,v) <- features)
       {
         if (!excluded.contains(k))
         {
           line ++= " "
           try 
           { 
             v.toDouble
             line ++= k
             line ++= ":"
             line ++= v
           }   
           catch 
           { 
             case _ => 
               {
                line ++= k
                line ++= "_"
                line ++= v
               }
           }
         }
       }
       line.toString()
     }
     vwLines
   }
 
  
  def run()
  {
     val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
     println("loading actions from "+actionsGlob)
      
     val rddFeatures = parseJsonFeatures(actionsGlob,config.targetFeature)
     
     val targets = rddFeatures.map{case (t,m) => t}.distinct().zipWithIndex().collectAsMap()
     

     val excluded = scala.collection.mutable.Set(config.excludedFeatures.split(","):_*)
     excluded.add(config.targetFeature)
     
     val vwLines = createVwLine(rddFeatures, targets, excluded.toSet,config.oaa)
     
     val vwOutPath = config.outputPath + "/" + config.client + "/vw/"+config.startDay
     FileUtils.outputModelToFile(vwLines, vwOutPath, DataSourceMode.fromString(vwOutPath), "vw.txt")
     
     val targetStr = new ArrayBuffer[String]()
     for ((k,v) <- targets)
     {
       var line = new StringBuilder()
       if (config.oaa)
         line ++= (v+1).toString()
       else
       {
         if (v == 1)
           line ++= "1"
         else
           line ++= "-1"
       }
       line ++= ","
       line ++= k
       targetStr.append(line.toString())
     }
     
     FileUtils.outputModelToFile(targetStr.toArray, vwOutPath, DataSourceMode.fromString(vwOutPath), "classes.txt")
     
  }
}

object CreateVwFeatures
{
   def updateConf(config : VwFeatureConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/vwfeatures"
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
         type DslConversion = VwFeatureConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[VwFeatureConfig] // extract case class from merged json
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

    var c = new VwFeatureConfig()
    val parser = new scopt.OptionParser[Unit]("CreateVwFeatures") {
    head("CreateVwFeatures", "1.0")
       opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        
        opt[String]("excludedFeatures") valueName("excluded features") foreach { x => c = c.copy(excludedFeatures = x) } text("features to exclude from json to vw translation")        
        opt[String]("targetFeature") valueName("target") foreach { x => c = c.copy(targetFeature = x) } text("the target feature")                
        opt[Boolean]("oaa") valueName("oaa") foreach { x => c = c.copy(oaa = x) } text("whether this is a one-against-all classification")                        

    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("CreateVwFeatures")

      if (c.local)
        conf.setMaster("local")
        .set("spark.akka.frameSize", "300")

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
        val cu = new CreateVwFeatures(sc,c)
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