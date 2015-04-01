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
package io.seldon.spark.topics

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.joda.time.format.DateTimeFormat
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import io.seldon.spark.SparkUtils

case class NextItemConfig(
    local : Boolean = false,
    client : String = "",    
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    jdbc : String = "",
    tagAttrId : Int = 9,  
    startDay : Int = 0,
    days : Int = 1,
    awsSecret : String = "",
    numItemTagsToKeep : Int = 400)
    
    
class CreateNextItemFeatures(private val sc : SparkContext,config : NextItemConfig) {

   def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      val dateUtc = (json \ "timestamp_utc").extract[String]
      
      val date = formatter.parseDateTime(dateUtc)
      
      
      (item,(user,date.getMillis()))
      }
    
    rdd
  }
   
   def getItemTagsFromDb(jdbc : String,tagAttrId : Int) = 
  {
    val sql = "select i.item_id,i.client_item_id,unix_timestamp(first_op),tags.value as tags from items i join item_map_varchar tags on (i.item_id=tags.item_id and tags.attr_id="+tagAttrId.toString()+") where i.item_id>? and i.item_id<?"
    val rdd = new org.apache.spark.rdd.JdbcRDD(
    sc,
    () => {
      Class.forName("com.mysql.jdbc.Driver")
      java.sql.DriverManager.getConnection(jdbc)
    },
    sql,
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"),row.getString("tags"))
    )
    rdd
  }


    def run()
    {
      val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
      println("loading actions from "+actionsGlob)
      
      val rddActions = parseJsonActions(actionsGlob)
      
      val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttrId)
      
      val rddCombined = rddActions.join(rddItems)
      
      // create feature for current item and the user history of items viewed
      val rddFeatures = rddCombined.map{ case (item,((user,time),tags)) => (user,(item,time,tags))}.groupByKey().flatMapValues{v =>
        val buf = new ListBuffer[String]()
        val sorted = v.toArray.sortBy(_._2)
        var userHistory = Set[String]()
        val s = sorted.size
        var pos = 0
        for ((item1,t1,tags1) <- sorted)
        {
          if (pos > 0){
          val (item2,t2,tags2) = sorted(pos-1)
          if (t2-t1 < 300)
          {
            var line = new StringBuilder()
            var tagSet = Set[String]()
            for(tag <- tags1.split(","))
            {
              if (!tagSet.contains(tag) && tag.trim().size > 0)
              {
                line ++= " item1_tag_"
                line ++= tag.trim().replaceAll(" ", "_")
                tagSet += tag
              }
            }
            tagSet = Set[String]()
            for(tag <- tags2.split(","))
            {
              if (!tagSet.contains(tag) && tag.trim().size > 0)
              {
                line ++= " item2_tag_"
                line ++= tag.trim().replaceAll(" ", "_")
                tagSet += tag
              }
            }
            buf.append(line.toString().trim())
          }
          }
          pos += 1
        }
         buf 
      }.cache()
      
      // create a set of ids for all features
      //val rddIds = rddFeatures.flatMap(_._2.split(" ")).distinct().zipWithUniqueId();

      val features = rddFeatures.flatMap(_._2.split(" ")).cache()
      
      val topItem1features = features.filter(_.startsWith("item1_tag_")).map(f => (f,1)).reduceByKey{case (c1,c2) => (c1+c2)}.map{case (f,c) => (c,f)}
        .sortByKey(false).take(config.numItemTagsToKeep)
      
      val topItem2features = features.filter(_.startsWith("item2_tag_")).map(f => (f,1)).reduceByKey{case (c1,c2) => (c1+c2)}.map{case (f,c) => (c,f)}
        .sortByKey(false).take(config.numItemTagsToKeep)

      
      var id = 1
      var idMap = collection.mutable.Map[String,Int]()
      for((c,f) <- topItem1features)
      {
        idMap.put(f, id)
        id += 1
      }
      for((c,f) <- topItem2features)
      {
        idMap.put(f, id)
        id += 1
      }

      //save feature id mapping to file
      val rddIds = sc.parallelize(idMap.toSeq,1)
      val idsOutPath = config.outputPath + "/" + config.client + "/feature_mapping/"+config.startDay
      rddIds.saveAsTextFile(idsOutPath)
      
      val broadcastMap = sc.broadcast(idMap)
      val broadcastTopItem1Features = sc.broadcast(topItem1features.map(v => v._2).toSet)
      val broadcastTopItem2Features = sc.broadcast(topItem2features.map(v => v._2).toSet)
      
      // map strings or orderd list of ids using broadcast map
      val rddFeatureIds = rddFeatures.map{v => 
        val tagToId = broadcastMap.value
        val validItem1Features = broadcastTopItem1Features.value
        val validItem2Features = broadcastTopItem2Features.value
        val features = v._2.split(" ")
        var line = new StringBuilder(v._1.toString()+" 1 ")
        var ids = ListBuffer[Long]()
        var item1FeaturesFound = false
        var item2FeaturesFound = false
        for (feature <- features)
        {
          if (validItem1Features.contains(feature) || validItem2Features.contains(feature))
            ids.append(tagToId(feature))
          if (!item2FeaturesFound && validItem2Features.contains(feature))
          {
            item2FeaturesFound = true
          }
          if (!item1FeaturesFound && validItem1Features.contains(feature))
          {
            item1FeaturesFound = true
          }
        }
        if (item1FeaturesFound && item2FeaturesFound)
        {
          ids = ids.sorted
          for (id <- ids)
          {
            line ++= " "
            line ++= id.toString()
            line ++= ":1"
          }
          line.toString().trim()
        }
        else
        {
          ""
        }
        }.filter(_.length()>3)
       
      val outPath = config.outputPath + "/" + config.client + "/features/"+config.startDay
      rddFeatureIds.saveAsTextFile(outPath)
    }
    
    
}


object CreateNextItemFeatures
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[NextItemConfig]("CreateNextItemFeatures") {
    head("CreateNextItemFeatures", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")    
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")    
    opt[Int]('t', "tagAttrId") required() action { (x, c) =>c.copy(tagAttrId = x) } text("tag attribute id in database")    
    opt[Int]('r', "numdays") required() action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") required() action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]("numItemTagsToKeep") required() action { (x, c) =>c.copy(numItemTagsToKeep = x) } text("number of top item tags to keep")

    }
    
    parser.parse(args, NextItemConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateNextItemFeatures")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)

      val cByd = new CreateNextItemFeatures(sc,config)
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