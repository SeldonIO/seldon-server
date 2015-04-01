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
	    local : Boolean = false,
	    client : String = "",
	    inputPath : String = "/seldon-models",
	    outputPath : String = "/seldon-models",
	    awsKey : String = "",
	    awsSecret : String = "",
	    startDay : Int = 1,
	    days : Int = 1,
      minSupport :Double = 0.005,
      minConfidence : Double = 0.2,
      minInterest : Double = 0.1
	   )

	class FPGrowthJob(private val sc : SparkContext,config : FPGrowthConfig) {

	  def parseSessions(path : String) = {
	    
	    val rdd = sc.textFile(path).map{line =>
	      
        val sessionIds = line.split("\\s+").map { x => x.toLong }
        
        sessionIds
	      }
	    
	    rdd
	  }

    def convertJson(assocRules : org.apache.spark.rdd.RDD[(Array[Long],Long,Double,Double)]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val json = assocRules.map{v =>
      val (itemset,item,confidence,interest) = v
      val json = (("itemset" -> itemset.mkString("[", ",", "]") ) ~
            ("item" -> item ) ~
            ("confidence" -> confidence) ~
            ("interest" -> interest))
       val jsonText = compact(render(json))    
       jsonText
    }
    json
  }

	  
	  def run()
	  {
	    val actionsGlob = config.inputPath + "/" + config.client+"/sessionitems/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
	    println("loading sessions from "+actionsGlob)
	      
	    val transactions = parseSessions(actionsGlob)
	    
      val numTransactions = transactions.count()
      
      println("number transaction "+numTransactions)
      
      val fpg = new FPGrowth()
      .setMinSupport(config.minSupport)
      .setNumPartitions(1)
      
      val model = fpg.run(transactions)
      
      val bNumtransactions = sc.broadcast(numTransactions)
      
      val itemsets = model.freqItemsets.map { itemset => 
          val numTransactions = bNumtransactions.value
          scala.util.Sorting.quickSort(itemset.items)
          (itemset.items.mkString("[", ",", "]"),itemset.freq/numTransactions.doubleValue())
        }
      
      // This might be too big. 
      val itemsetMap = itemsets.collectAsMap()
      
      val bItemSetMap = sc.broadcast(itemsetMap)
      

      val minConfidence = config.minConfidence
      val minInterest = config.minInterest
      
      // Go through item sets and mine association rules
      val assocRules = model.freqItemsets.flatMap { itemset => 
        if (itemset.items.length > 1)
        {
          val numTransactions = bNumtransactions.value
          val buf = new ListBuffer[(Array[Long],Long,Double,Double)]()
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
                println("support union :"+supp.toString()+" supp antecedent : "+suppA.toString())
                val confidence = supp/suppA
                val interest = confidence - suppItem
                if (confidence > minConfidence && interest > minInterest)
                {
                  buf.append((antecedent,item,confidence,interest))
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
	  def main(args: Array[String]) 
	  {

	    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

	    val parser = new scopt.OptionParser[FPGrowthConfig]("ClusterUsersByDimension") {
	    head("CrateVWTopicTraining", "1.x")
	    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
	    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
	    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
	    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
	    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
	    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
	    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
	    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
	      
	    }
	    
	    parser.parse(args, FPGrowthConfig()) map { config =>
	    val conf = new SparkConf()
	      .setAppName("CreateVWTopicTraining")
	      
	    if (config.local)
	      conf.setMaster("local")
	    
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
	      val cByd = new FPGrowthJob(sc,config)
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
