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
import io.seldon.spark.SparkUtils
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import scala.util.Random

case class SessionItemsConfig(
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
    allowedTypes : String = "")

class SessionItems(private val sc : SparkContext,config : SessionItemsConfig) {

  def parseJsonActions(path : String,allowedTypes : Set[Int]) = {
    
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

      if (allowedTypes.size == 0 || (allowedTypes.contains(actionType) ))
      {
        val date1 = org.joda.time.format.ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(dateUtc)
        Seq((user,(item,date1.getMillis())))
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
    var allowedTypes = Set[Int]()
    if (config.allowedTypes.nonEmpty)
    {
       allowedTypes = config.allowedTypes.split(",").map(_.toInt).distinct.toSet 
    }
    
      
    for(t <- allowedTypes)
      println("type ",t)
    val rddActions = parseJsonActions(actionsGlob,allowedTypes)
    
    val minNumActions = config.minActionsPerUser
    val maxNumActions = config.maxActionsPerUser
    val maxGapMsecs = config.maxIntraSessionGapSecs * 1000
      // create feature for current item and the user history of items viewed
    val rddFeatures = rddActions.groupByKey().filter(_._2.size >= minNumActions).filter(_._2.size <= maxNumActions).flatMapValues{v =>
        val buf = new ListBuffer[String]()
        var line = new StringBuilder()
        val sorted = v.toArray.sortBy(_._2)
        var lastTime : Long = 0
        var timeSecs : Long = 0
        for ((item,t) <- sorted)
        {
          if (lastTime > 0)
          {
            val gap = (t - lastTime)
            if (maxGapMsecs > -1 && gap > maxGapMsecs)
            {
              val lineStr = line.toString().trim()
              if (lineStr.length() > 0)
                buf.append(line.toString().trim())
              line.clear()
            }
          }
          line ++= item.toString() + " "
          lastTime = t
         } 
         val lineStr = line.toString().trim()
         if (lineStr.length() > 0)
           buf.append(lineStr)
         buf 
      }.values
      val outPath = config.outputPath + "/" + config.client + "/sessionitems/"+config.startDay
      rddFeatures.saveAsTextFile(outPath)
  }
}

 object SessionItems
{
   
  def updateConf(config : SessionItemsConfig) =
  {
    import io.seldon.spark.zookeeper.ZkCuratorHandler
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/sessionitems"
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
         type DslConversion = SessionItemsConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[SessionItemsConfig] // extract case class from merged json
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

    var c = new SessionItemsConfig()
    val parser = new scopt.OptionParser[Unit]("SessionItems") {
    head("SessionItems", "1.0")
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
        opt[String]("allowedTypes") foreach { x => c = c.copy(allowedTypes = x) } text("Allowed action types - ignore actions not of this comma separated list")                
    }
    
     if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

       val conf = new SparkConf().setAppName("SessionItems")

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
        val si = new SessionItems(sc,c)
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