package io.seldon.spark.recommend

/**
 * @author clive
 */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import util.Random.nextInt
import java.io.File
import java.sql.{DriverManager,ResultSet}
import collection.mutable.{ HashMap, MultiMap, Set }
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.Period
import io.seldon.spark.SparkUtils


case class DimCount(m : scala.collection.mutable.Map[Int,Int],total : Int)
case class DimPercent(dim : Int,percent : Float)
case class MostPopularByDimConfig(
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

    jdbc : String = "",
    k : Int = 1000,
    minCount : Int = 0,
    requiredDimensions : String = "")

class MostPopularByDimJob(private val sc : SparkContext,config : MostPopularByDimConfig) {
 
  def activate(location : String) 
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    import org.apache.curator.utils.EnsurePath
    val curator = new ZkCuratorHandler(config.zkHosts)
    if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
    {
        val zkPath = "/all_clients/"+config.client+"/mostpopulardim"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,location.getBytes())
    }
    else
      println("Failed to get zookeeper! Can't activate model")
  }
  
  /*
   * Get the dimensions from the database
   */
  def getDimsFromDb(jdbc : String) = 
  {
    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    "select item_id,d.dim_id from item_map_enum i join dimension d on (i.attr_id=d.attr_id and i.value_id=d.value_id) where d.dim_id<65535 and item_id > ? AND item_id <= ?",
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getInt("dim_id"))
    )
    rdd
  }
  
   def parseJson(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val item = (json \ "itemid").extract[Int]
      val value = (json \ "value").extract[Float]
      (item,value+1)
      }
    
    rdd
  }
  
   
  def convertJson(mostPop : org.apache.spark.rdd.RDD[(Int,(Int,Float))]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val userJson = mostPop.map{case (dim,(item,count))  =>

      val json = (("dim" -> dim ) ~
            ("item" -> item ) ~
            ("count" -> count))
       val jsonText = compact(render(json))    
       jsonText
    }
    userJson
  }
  
  def run()
  {

    // get item_id -> dimension
    val itemDim = getDimsFromDb(config.jdbc)
    
    val validDims = if (config.requiredDimensions == "") { Set[Int]() } else {config.requiredDimensions.split(",").map { x => x.toInt }.toSet }

    val itemDimValid = itemDim.groupByKey().filter(validDims.size == 0 || _._2.toSet.intersect(validDims).size > 0).flatMapValues(_.toList)  

    

    val glob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading from "+glob)

    // parse json from inputs
    val rddItem = parseJson(glob)
    val k = config.k
    val minCount = config.minCount  
    val itemsDim = itemDimValid.join(rddItem).map{case (item, (dim,value)) => ((item,dim), value) }.
      reduceByKey((x, y) => (x+y)).
      filter(_._2 >= minCount).
      map{case ((item,dim),value) => (dim,(item,value))}.
      combineByKey( // key top k for each dimension
          d => List[(Int,Float)](d),
          (x:List[(Int,Float)],d:(Int,Float))=>(d :: x).sortBy(-_._2).take(k), 
          (x:List[(Int,Float)],y:List[(Int,Float)]) => (x ::: y).sortBy(-_._2).take(k)
        ).flatMapValues(v=> v)

    val json = convertJson(itemsDim)
    
    val outPath = config.outputPath + "/" + config.client + "/mostpopulardim/"+config.startDay
    
    json.coalesce(1, false).saveAsTextFile(outPath)
    
    if (config.activate)
       activate(outPath)
  }
}

object MostPopularByDimJob
{
   def updateConf(config : MostPopularByDimConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/mostpopulardim"
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
         type DslConversion = MostPopularByDimConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[MostPopularByDimConfig] // extract case class from merged json
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

    var c = new MostPopularByDimConfig()
    val parser = new scopt.OptionParser[Unit]("MostPopularByDimJob") {
    head("MostPopularByDimJob", "1.0")
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
        opt[Int]('k', "k") foreach { x => c = c.copy(k = x) } text("top n for each dim to keep")
        opt[Int]('m', "minCount") foreach { x => c = c.copy(minCount = x) } text("min action count for item")
        opt[String]("requiredDimensions") foreach { x => c = c.copy(requiredDimensions = x) } text("comma separated list of required dimensions for items")
    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("MostPopularByDimJob")

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
        val cu = new MostPopularByDimJob(sc,c)
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