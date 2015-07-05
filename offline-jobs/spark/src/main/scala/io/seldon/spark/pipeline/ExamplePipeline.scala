package io.seldon.spark.pipeline

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.joda.time.format.DateTimeFormat
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import io.seldon.spark.SparkUtils
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.joda.time.Duration
import org.joda.time.LocalDateTime
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode
import scala.collection.mutable.ListMap
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.VectorAssembler


case class ExConfig(
    local : Boolean = false,
    client : String = "",    
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    startDay : Int = 0,
    days : Int = 1,
    awsSecret : String = "",
    zkHosts : String = "")
    
    
class ExamplePipeline(private val sc : SparkContext,config : ExConfig) {

   def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val json = parse(line)

      val values = json.extract[scala.collection.Map[String, _]]
      for((key,value) <- values)
      {
        
      }
      
      }
    
    rdd
  }
   
    


    def run()
    {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val actionsGlob = config.inputPath + "/" + config.client+"/events/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
      println("loading actions from "+actionsGlob)
      val df = sqlContext.read.json(actionsGlob)
      
      df.printSchema()
      df.show()
      
      /*
       * Example pipeline that tokenizing and creates tfidf vector then illustrate joining the tfidf and features columns into a single vector column
       */
      
      val tokenizer = new Tokenizer()
        .setInputCol("product_description")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
//        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val idf = new IDF()
        .setInputCol(hashingTF.getOutputCol)
        .setOutputCol("tfidf")
      val vectorizer1 = new VectorAssembler()
        .setInputCols(Array("tfidf","features"))
        .setOutputCol("word_features")
        
       
      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF,idf,vectorizer1))
      val model = pipeline.fit(df)
      val dfOut = model.transform(df)
        
      dfOut.printSchema()
      dfOut.show()
      
      /*
       * Second pipeline that standarizes a numeric column - needs to create a vector from the column(s) first
       */
      val vectorizer = new VectorAssembler()
        .setInputCols(Array("median_relevance"))
        .setOutputCol("vec_col")
      val scaler = new StandardScaler()
        .setInputCol(vectorizer.getOutputCol)
        .setOutputCol("vec_col_scaled")
        .setWithMean(true)
        .setWithStd(true)
      
      val pipeline2 = new Pipeline().setStages(Array(vectorizer,scaler))
      val model2 = pipeline2.fit(df.select("id", "median_relevance"))
      val dfOut2 = model2.transform(df.select("id", "median_relevance"))

      dfOut2.printSchema()
      dfOut2.show()

      // Illustrates joining 2 dataframes to get a single dataframe to save
      //val dfFinal = dfOut.join(dfOut2, dfOut("id") === dfOut2("id"))
      
      val outPath = config.outputPath + "/" + config.client + "/features/"+config.startDay
      dfOut.toJSON.saveAsTextFile(outPath)
    }
    
    
}


object ExamplePipeline
{
   def updateConf(config : ExConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/cluster-by-dimension"
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
         type DslConversion = ExConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[ExConfig] // extract case class from merged json
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

    var c = new ExConfig()
    val parser = new scopt.OptionParser[Unit]("ExamplePipeline") {
    head("ExamplePipeline", "1.0")
       opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
    
    }
    
      if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("ExamplePipeline")

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
        val cu = new ExamplePipeline(sc,c)
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