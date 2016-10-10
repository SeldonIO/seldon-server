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
package io.seldon.spark.mllib

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import io.seldon.spark.SparkUtils
import scala.util.Random
import io.seldon.spark.zookeeper.ZkCuratorHandler

case class SimilarItemsConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",
    
    itemType : Int = -1,
    limit : Int = 100,
    minItemsPerUser : Int = 0,
    minUsersPerItem : Int = 0,    
    maxUsersPerItem : Int = 2000000,    
    dimsumThreshold : Double = 0.1,
    sample : Double = 1.0

)
    
class SimilarItems(private val sc : SparkContext,config : SimilarItemsConfig) {

  
  def parseJson(path : String,itemType : Int,sample : Double) = {
    
    val rdd = sc.textFile(path).flatMap{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val rand = new Random()    
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      val itype = (json \ "type").extract[Int]
      if (itemType == -1 || itype == itemType)
      {
        if (rand.nextDouble() < sample)
          Seq((item,user))
        else
          None
      }
      else
        None
      }
    
    rdd
  }
  
  def sortAndLimit(similarities : org.apache.spark.rdd.RDD[MatrixEntry],limit : Int) = {
    val v = similarities.map{me => (me.i,(me.j,me.value))}.groupByKey().mapValues(_.toSeq.sortBy{ case (domain, count) => count }(Ordering[Double].reverse).take(limit)).flatMapValues(v => v)
    v
  }
  
  def convertJson(similarities : org.apache.spark.rdd.RDD[(Long,(Long,Double))]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    similarities.map{me =>
      val json = (("item1" -> me._1 ) ~
            ("item2" -> me._2._1 ) ~
            ("sim" -> me._2._2))
       val jsonText = compact(render(json))    
       jsonText
    }

  }
  
  def convertJsonFromMatrixEntry(similarities : org.apache.spark.rdd.RDD[MatrixEntry]) = {
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    similarities.map{me =>
      val json = (("item1" -> me.i ) ~
            ("item2" -> me.j ) ~
            ("sim" -> me.value))
       val jsonText = compact(render(json))    
       jsonText
    }

  }
  
  def filterItems(rdd : org.apache.spark.rdd.RDD[(Int,Int)],minUsersPerItem : Int,maxUsersPerItem : Int) : org.apache.spark.rdd.RDD[(Int,Int)] = 
  {
       rdd.distinct().groupBy(_._1).filter(_._2.size >= minUsersPerItem).filter(_._2.size <= maxUsersPerItem).flatMap(_._2).cache()
  }
  
  def getUserVectors(rdd : org.apache.spark.rdd.RDD[(Int,Int)],minItemsPerUser : Int,maxItem :Int) : org.apache.spark.rdd.RDD[Vector] =
  {
    rdd.groupByKey().filter(_._2.size >= minItemsPerUser)
     .map{ case (user,items) =>
      Vectors.sparse(maxItem, items.map(item => (item,1.toDouble)).toSeq)
      }
  }
  
  def runDimSum(r :RowMatrix,dimsumThreshold : Double) : org.apache.spark.rdd.RDD[MatrixEntry] =
  {
    r.columnSimilarities(dimsumThreshold).entries
  }
  
  
  
  def run()
  {

    val glob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading from "+glob)
    
    val rddJson = parseJson(glob,config.itemType,config.sample)
    
    val itemsFiltered = filterItems(rddJson, config.minUsersPerItem, config.maxUsersPerItem)

    val numItems = itemsFiltered.keys.distinct().count()
    println("num items : "+numItems)
    
    val maxItem = itemsFiltered.keys.max() + 1
    
    val users = itemsFiltered.map{case (item,user) => (user,item)}

    val userVectors = getUserVectors(users, config.minItemsPerUser, maxItem)
    
    val numUsers = userVectors.count()
    println("Number of users : "+numUsers)
    
    val r = new RowMatrix(userVectors);
    
    println("Running item similarity with threshold :"+config.dimsumThreshold)
    val simItems = runDimSum(r, config.dimsumThreshold)
    
    //val json = convertJson(simItems)
    
    val json = convertJson(sortAndLimit(simItems, config.limit))
    
    val outPath = config.outputPath + "/" + config.client + "/item-similarity/"+config.startDay
    
    json.saveAsTextFile(outPath)
   

    
  }
  
}


object SimilarItems
{
   def updateConf(config : SimilarItemsConfig) =
  {
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/similar-items"
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
         type DslConversion = SimilarItemsConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[SimilarItemsConfig] // extract case class from merged json
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
    
    var c = new SimilarItemsConfig()
    val parser = new scopt.OptionParser[Unit]("SimilarItems") {
    head("ClusterUsersByDimension", "1.x")
        opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]('e', "itemType") foreach { x =>c = c.copy(itemType = x) } text("item type to limit foreachs to")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[Int]('u', "minUsersPerItem") foreach { x =>c = c.copy(minUsersPerItem = x) } text("min number of users to interact with an item")
        opt[Int]('m', "maxUsersPerItem") foreach { x =>c = c.copy(maxUsersPerItem = x) } text("max number of users to interact with an item")
        opt[Int]('p', "minItemsPerUser") foreach { x =>c = c.copy(minItemsPerUser = x) } text("min number of items a user needs to interact with")
        opt[Int]('l', "limit") foreach { x =>c = c.copy(limit = x) } text("keep top N similarities per item")
        opt[Double]('d', "dimsumThreshold") foreach { x =>c = c.copy(dimsumThreshold = x) } text("min cosine similarity estimate for dimsum (soft limit)")
        opt[Double]('s', "sample") foreach { x =>c = c.copy(sample = x) } text("what percentage of the input data to use, values in range 0.0..1.0, defaults to 1.0 (use all the data)")        
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
    }
    
    
    if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line
      
      val conf = new SparkConf().setAppName("SimilarItems")

      if (c.local)
        conf.setMaster("local")
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
        val si = new SimilarItems(sc,c)
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