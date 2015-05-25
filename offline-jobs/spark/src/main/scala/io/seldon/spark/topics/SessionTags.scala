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

case class SessionTagsConfig(
    local : Boolean = false,
    client : String = "",
    jdbc : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    days : Int = 1,
    tagAttr : String = "tags",
    maxIntraSessionGapSecs : Int = 600,
    minTagsInSession : Int = 4,
    minActionsPerUser : Int = 3,
    maxActionsPerUser : Int = 100)

class SessionTags(private val sc : SparkContext,config : SessionTagsConfig) {

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
      
      val date1 = org.joda.time.format.ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(dateUtc)
      //val date = formatter.parseDateTime(dateUtc)
      
      
      (item,(user,date1.getMillis()))
      }
    
    rdd
  }
  
    def getItemTagsFromDb(jdbc : String,attr : String) = 
  {
    //val sql = "select i.item_id,i.client_item_id,unix_timestamp(first_op),tags.value as tags from items i join item_map_"+table+" tags on (i.item_id=tags.item_id and tags.attr_id="+tagAttrId.toString()+") where i.item_id>? and i.item_id<?"
    val sql = "select * from (SELECT i.item_id,i.client_item_id,unix_timestamp(first_op),CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END" +  
      " tags FROM  items i INNER JOIN item_attr a ON a.name in ('"+attr+"') and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id " +
      " where i.item_id>? and i.item_id<? order by imv.pos) t where not t.tags is null"
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
    
    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
      
      val rddCombined = rddActions.join(rddItems)
      
      val minNumActions = config.minActionsPerUser
      val maxNumActions = config.maxActionsPerUser
      val maxGapMsecs = config.maxIntraSessionGapSecs * 1000
      val minTagsInSession = config.minTagsInSession
      // create feature for current item and the user history of items viewed
      val rddFeatures = rddCombined.map{ case (item,((user,time),tags)) => (user,(item,time,tags))}.groupByKey().filter(_._2.size > minNumActions).filter(_._2.size < maxNumActions).flatMapValues{v =>
        val buf = new ListBuffer[String]()
        var line = new StringBuilder()
        val sorted = v.toArray.sortBy(_._2)
        var lastTime : Long = 0
        var timeSecs : Long = 0
        var numTags = 0
        for ((item,t,tags) <- sorted)
        {
          if (lastTime > 0)
          {
            val gap = (t - lastTime)
            if (gap > maxGapMsecs)
            {
              val lineStr = line.toString().trim()
              if (lineStr.length() > 0 && line.toString().split(" ").length >= minTagsInSession)
                buf.append(line.toString().trim())
              line.clear()
            }
            else
            {
              line ++= " "
            }
            numTags = 0
          }
          //val tagList = Random.shuffle(tags.split(",").toList)
          val tagList = tags.split(",")
          for(tag <- tagList)
          {
            if (tag.trim().length() > 0)
            {
              if (numTags > 0)
               line ++= " "
              line ++= tag.trim().toLowerCase().replaceAll(" ", "_")
              numTags += 1
            }
          }
          lastTime = t
         } 
         val lineStr = line.toString().trim()
         if (lineStr.length() > 0 && line.toString().split(" ").length >= minTagsInSession)
           buf.append(lineStr)
         buf 
      }.values
      val outPath = config.outputPath + "/" + config.client + "/sessiontags/"+config.startDay
      rddFeatures.saveAsTextFile(outPath)
  }
}

 object SessionTags
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[SessionTagsConfig]("SesisonTags") {
    head("SessionTags", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[String]('t', "tagAttr") required() valueName("tag attr") action { (x, c) => c.copy(tagAttr = x) } text("attr name in db containing tags")    
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[String]('a', "awskey")  valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret")  valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]("minActionsPerUser") action { (x, c) =>c.copy(minActionsPerUser = x) } text("min number of actions per user")
    opt[Int]("maxActionsPerUser") action { (x, c) =>c.copy(maxActionsPerUser = x) } text("max number of actions per user")    
    opt[Int]("minTagsInSession") action { (x, c) =>c.copy(minTagsInSession = x) } text("min number of tags in session")
    opt[Int]("maxIntraSessionGapSecs") action { (x, c) =>c.copy(maxIntraSessionGapSecs = x) } text("max intra session gap secs")

    }
    
    parser.parse(args, SessionTagsConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("SessionTags")
      
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

      val cByd = new SessionTags(sc,config)
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