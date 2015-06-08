package io.seldon.spark.transactions

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import io.seldon.spark.SparkUtils
import scala.collection.mutable.ListBuffer


case class FPGrowthConfig(
	    client : String = "",
	    inputPath : String = "/seldon-models",
      inputSubFolder : String = "basketanalysis",
	    outputPath : String = "/seldon-models",
	    awsKey : String = "",
	    awsSecret : String = "",
	    startDay : Int = 1,
	    days : Int = 1,
      zkHosts : String = "",
      local : Boolean = false,      
      activate : Boolean = false,
      
      minSupport :Double = 0.005, // percentage of total transactions for an itemset to be included
      minConfidence : Double = 0.2, // the min percentage for the itemset as compared to its antecedent for the rule itemset,itemset+item -> item
      minInterest : Double = 0.1, // confidence minus support for item to be recommended
      minLift : Double = 0.0, // min lift for rule to be included
      minItemSetFreq : Int = 0, // min absolute freq of an itemset to be included
      maxItemSetSize : Int = 5 // max length of an itemset to be included
	   )

	class FPGrowthJob(private val sc : SparkContext,config : FPGrowthConfig) {

	  def parseSessions(path : String) = {
	    
	    val rdd = sc.textFile(path).map{line =>
	      
        val sessionIds = line.split("\\s+").map { x => x.toLong }
        
        sessionIds
	      }
	    
	    rdd
	  }

    def convertJson(assocRules : org.apache.spark.rdd.RDD[(Array[Long],Long,Double,Double,Double)]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val json = assocRules.map{v =>
      val (itemset,item,confidence,interest,lift) = v
      val json = (("itemset" -> itemset.mkString("[", ",", "]") ) ~
            ("item" -> item ) ~
            ("confidence" -> confidence) ~
            ("lift" -> lift) ~
            ("interest" -> interest))
       val jsonText = compact(render(json))    
       jsonText
    }
    json
  }

	  
	  def run()
	  {
	    val actionsGlob = config.inputPath + "/" + config.client+"/"+config.inputSubFolder+"/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
	    println("loading sessions from "+actionsGlob)
	      
	    val transactions = parseSessions(actionsGlob)
	    
      val numTransactions = transactions.count()
      
      println("number transaction "+numTransactions)
      
      val fpg = new FPGrowth()
      .setMinSupport(config.minSupport)
      .setNumPartitions(1)
      
      
      val model = fpg.run(transactions)
      
      println("fp growth done")
      
      val bNumtransactions = sc.broadcast(numTransactions)
      
      println("broadcast")
      val minItemSetFreq = config.minItemSetFreq
      val maxItemSetSize = config.maxItemSetSize
      val itemsets = model.freqItemsets.flatMap { itemset => 
          if (itemset.freq > minItemSetFreq && itemset.items.length <= maxItemSetSize)
          {
            val numTransactions = bNumtransactions.value
            scala.util.Sorting.quickSort(itemset.items)
            val key = itemset.items.mkString("[", ",", "]")
            println(key,itemset.freq)
            Seq((key,itemset.freq/numTransactions.doubleValue()))
          }
          else
            None
        }
      

      val numItemSets = itemsets.count();
      println("num itemsets "+numItemSets)
      // This might be too big. 
      val itemsetMap = itemsets.collectAsMap()

      println("local map created")      
      
      val bItemSetMap = sc.broadcast(itemsetMap)
      

      val minConfidence = config.minConfidence
      val minInterest = config.minInterest
      
      // Go through item sets and mine association rules
      val assocRules = model.freqItemsets.flatMap { itemset => 
        if (itemset.items.length > 1)
        {
          val numTransactions = bNumtransactions.value
          val buf = new ListBuffer[(Array[Long],Long,Double,Double,Double)]()
          val amap = bItemSetMap.value
          scala.util.Sorting.quickSort(itemset.items)
          for(item <- itemset.items)
          {
            val antecedent = itemset.items.filterNot { x => x == item }
            val key = antecedent.mkString("[", ",", "]")
            println("looking at antecedent "+key+" and item "+item.toString())
            if (amap.contains(key))
            {
              val anteSupp = amap.getOrElse(key, 0D)
              if (anteSupp > 0.0)
              {
                val supp = itemset.freq/numTransactions.doubleValue()
                val suppA = anteSupp
                val suppItem = amap.getOrElse("["+item.toString()+"]", 0.0)
                if (suppItem > 0.0)
                {
                  println("support union :"+supp.toString()+" supp antecedent : "+suppA.toString())
                  val confidence = supp/suppA
                  val interest = confidence - suppItem
                  val lift = confidence/suppItem
                  if (confidence > minConfidence && interest > minInterest)
                  {
                    buf.append((antecedent,item,confidence,interest,lift))
                  }
                }
              }
            }
          }
          buf
        }
        else
          None
      }
      
      val json = convertJson(assocRules)
	    
      val outPath = config.outputPath + "/" + config.client + "/fpgrowth/"+config.startDay
    
      json.saveAsTextFile(outPath);


	  }
	}


object FPGrowthJob
{
   def updateConf(config : FPGrowthConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/fpgrowth"
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
         type DslConversion = FPGrowthConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[FPGrowthConfig] // extract case class from merged json
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

    var c = new FPGrowthConfig()
    val parser = new scopt.OptionParser[Unit]("FPGrowth") {
    head("FPGrowth", "1.0")
       opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]("inputSubFolder") valueName("input sub folder") foreach { x => c = c.copy(inputSubFolder = x) } text("input sub folder to use")        
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")

        opt[Double]("minSupport") foreach { x =>c = c.copy(minSupport = x) } text("min support")        
        opt[Double]("minConfidence") foreach { x =>c = c.copy(minConfidence = x) } text("min confidence")        
        opt[Double]("minInterest") foreach { x =>c = c.copy(minInterest = x) } text("min interest")                
        opt[Double]("minLift") foreach { x =>c = c.copy(minLift = x) } text("min lift")                        
        opt[Int]("minItemSetFreq") foreach { x =>c = c.copy(minItemSetFreq = x) } text("min item set freq")                                
        opt[Int]("maxItemSetSize") foreach { x =>c = c.copy(maxItemSetSize = x) } text("max item set size")                                        
    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("FPGrowth")

      if (c.local)
        conf.setMaster("local")
        .set("spark.akka.frameSize", "300")
         .set("spark.executor.memory", "8g")

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
        val cu = new FPGrowthJob(sc,c)
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


