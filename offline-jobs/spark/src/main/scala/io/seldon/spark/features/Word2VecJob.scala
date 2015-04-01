/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.spark.features

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd._
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import io.seldon.spark.SparkUtils
import java.io.File
import io.seldon.spark.rdd.DataSourceMode
import io.seldon.spark.rdd.DataSourceMode._
import io.seldon.spark.rdd.FileUtils
    
case class Word2VecConfig (
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
    
    minWordCount : Int = 50,
    vectorSize : Int = 30)

 
class Word2VecJob(private val sc : SparkContext,config : Word2VecConfig) {

  def activate(location : String) 
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    import org.apache.curator.utils.EnsurePath
    val curator = new ZkCuratorHandler(config.zkHosts)
    if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
    {
        val zkPath = "/all_clients/"+config.client+"/word2vec"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,location.getBytes())
    }
    else
      println("Failed to get zookeeper! Can't activate model")
  }
    
  def convertToSemVecFormat(vectors : org.apache.spark.rdd.RDD[(String,Array[Float])],dimension : Int) = 
  {
    val vecString = vectors.coalesce(1, true).map{v =>
      val key = v._1.replaceAll(",", "")
      var line = new StringBuilder()
      line ++= key
      for (score <- v._2)
      {
        line ++= "|"
        line ++= score.toString()
      }
      line.toString()
    }
    val header: RDD[String] = sc.parallelize(Array("-vectortype REAL -dimension "+dimension.toString()))
    val vectorStr = header.union(vecString).coalesce(1, true)
    vectorStr
  }
 
  def run()
  {

    val glob = config.inputPath + "/" + config.client+"/sessionitems/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"

    println("loading from "+glob)
    
   val input = sc.textFile(glob).map(line => line.split(" ").toSeq)
   val word2vec = new SeldonWord2Vec()
   word2vec.setMinCount(config.minWordCount)
   word2vec.setVectorSize(config.vectorSize)
   val model = word2vec.fit(input)
   val vectors = model.getVectors
   val rdd = sc.parallelize(vectors.toSeq, 200)

   val outPath = config.outputPath + "/" + config.client + "/word2vec/"+config.startDay

   val mode = DataSourceMode.fromString(config.outputPath)

   val vectorsAsString = convertToSemVecFormat(rdd,config.vectorSize)
   FileUtils.outputModelToFile(vectorsAsString, outPath, mode, "docvectors.txt")
   val emptyTermFile: RDD[String] = sc.parallelize(Array("-vectortype REAL -dimension "+config.vectorSize.toString()))
   FileUtils.outputModelToFile(emptyTermFile, outPath, mode, "termvectors.txt")
   if (config.activate)
     activate(outPath)
  }
 
}


object Word2VecJob
{
  def updateConf(config : Word2VecConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/word2vec"
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
         type DslConversion = Word2VecConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[Word2VecConfig] // extract case class from merged json
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

    var c = new Word2VecConfig()
    val parser = new scopt.OptionParser[Unit]("Word2Vec") {
    head("Word2Vec", "1.0")
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
        
        opt[Int]("minWordCount") foreach { x => c = c.copy(minWordCount = x) } text("min count for a token to be included")
        opt[Int]("vectorSize") foreach { x => c =c.copy(vectorSize = x) } text("vector size")    
    }
    
    if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("Word2Vec")

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
        val w2v = new Word2VecJob(sc,c)
        w2v.run()
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