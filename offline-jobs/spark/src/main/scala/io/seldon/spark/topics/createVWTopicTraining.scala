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

case class Config(
    local : Boolean = false,
    client : String = "",
    jdbc : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    awsKey : String = "",
    awsSecret : String = "",
    startDay : Int = 1,
    days : Int = 1,
    tagAttr : String = "",
    minActionsPerUser : Int = 10)

class createVWTopicTraining(private val sc : SparkContext,config : Config) {

  def parseJsonActions(path : String) = {
    
    val rdd = sc.textFile(path).map{line =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
    
      val json = parse(line)
      val user = (json \ "userid").extract[Int]
      val item = (json \ "itemid").extract[Int]
      (user,item)
      }
    
    rdd
  }
  
   def getItemTagsFromDb(jdbc : String,attr : String) = 
  {
//    val sql = "select i.item_id,i.client_item_id,unix_timestamp(first_op),tags.value as tags from items i join item_map_"+table+" tags on (i.item_id=tags.item_id and tags.attr_id="+tagAttrId.toString()+") where i.item_id>? and i.item_id<?"
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
    (row : ResultSet) => (row.getInt("item_id"),row.getString("tags").toLowerCase().trim())
    )
    rdd
  }
  
  def getFilteredActions(minActions : Int,actions : org.apache.spark.rdd.RDD[(Int,Int)]) = {

    actions.groupBy(_._1).filter(_._2.size >= minActions).flatMap(_._2).map(v => (v._2,v._1)) // filter users with no enough actions and transpose to item first
  }
  
  def run()
  {
    val actionsGlob = config.inputPath + "/" + config.client+"/actions/"+SparkUtils.getS3UnixGlob(config.startDay,config.days)+"/*"
    println("loading actions from "+actionsGlob)
    println("Loading tags from "+config.jdbc)
    val rddActions = getFilteredActions(config.minActionsPerUser, parseJsonActions(actionsGlob))

    // get item tags from db

    val rddItems = getItemTagsFromDb(config.jdbc, config.tagAttr)
    
    // sort by item_id and join actions and tags
    val rddCombined = rddActions.join(rddItems)
    
    // get user,tag,count triples
    val userTagCounts = rddCombined.map(_._2).flatMapValues(_.split(",")).mapValues(_.trim()).filter(_._2.size > 0)
      .map(v=>((v._1,v._2),(v._2,1))).sortByKey().reduceByKey((v1,v2) => (v1._1,v1._2+v2._2))
       .map(v => (v._1._1,v._2)).sortByKey().map(v => v._1.toString()+","+v._2._1+","+v._2._2.toString())
    
    val outPath = config.outputPath + "/" + config.client + "/user_tag_count/"+config.startDay
    
    userTagCounts.saveAsTextFile(outPath)
  }
}

 object createVWTopicTraining
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[Config]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")
    opt[String]('i', "input-path") valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")
    opt[Int]('r', "numdays") action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[String]('t', "tagAttr") required() valueName("tag attr") action { (x, c) => c.copy(tagAttr = x) } text("attr name in db containing tags")    
    opt[Int]("start-day") action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[String]('a', "awskey") valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('m', "minActionsPerUser") action { (x, c) =>c.copy(minActionsPerUser = x) } text("min number of actions per user")
    }
    
    parser.parse(args, Config()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateVWTopicTraining")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
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
      val cByd = new createVWTopicTraining(sc,config)
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