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
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.joda.time.Duration
import org.joda.time.LocalDateTime
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import io.seldon.spark.rdd.FileUtils
import io.seldon.spark.rdd.DataSourceMode
import scala.util.control.Breaks._

case class ActionConfig(
    local : Boolean = false,
    client : String = "",    
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    jdbc : String = "",
    tagAttr : String = "tag",  
    startDay : Int = 0,
    days : Int = 1,
    awsSecret : String = "",
    maxNumActionsPerUser : Int = 25,
    actionNumToStart : Int = 5,
    minTermDocFreq : Int = 100)
    
    
class CreateActionFeatures(private val sc : SparkContext,config : ActionConfig) {

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
      //val date = formatter.parseLocalDateTime(dateUtc)

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
      
      val countCombined = rddCombined.count()
      println("actions with tags count is "+countCombined.toString())
      
      val maxNumActions = config.maxNumActionsPerUser
      val actionNumToStart = config.actionNumToStart
      
      println("max actions "+maxNumActions.toString()+" start at "+actionNumToStart.toString())
      // create feature for current item and the user history of items viewed
      val rddFeatures = rddCombined.map{ case (item,((user,time),tags)) => (user,(item,time,tags))}.groupByKey()
          .flatMapValues{v =>
        val buf = new ListBuffer[String]()
        val sorted = v.toArray.sortBy(_._2) // _.2 is time
        var userHistory = ListBuffer[String]()
        var c = 0
        breakable { for ((item,t,tags) <- sorted)
        {
          if (c > maxNumActions)
            break;
          if (c >= actionNumToStart)
          {
            var line = new StringBuilder()
            for(tag <- tags.split(","))
           {
             val tagToken = tag.trim().toLowerCase().replaceAll("[ :;'\",]", "_")
            // create a set of item tag features for each tag in current item
            if (tagToken.size > 0)
            {
              line ++= " i_"
              line ++= tagToken
             }
            }
            // create a set if user tag features for each tag in user history
            for (tag <- userHistory)
            {
             if (tag.size > 0)
             {
              line ++= " u_"
              line ++= tag
             }  
            }
            buf.append(line.toString().trim())
           }
           // add all tags from current item to user history
           for (tag <- tags.split(","))
           {
             val tagToken = tag.trim().toLowerCase().replaceAll("[ :;'\",]", "_")
             if (tagToken.size > 0)
                userHistory.append(tagToken)
            }
          c += 1
         }
        }
         buf 
      }

      val countFeatures = rddFeatures.count()
      println("Count of rddFeatures "+countFeatures.toString())
      
      val featuresIter = rddFeatures.map(_._2.split(" ").toSeq)

      val hashingTF = new HashingTF()
      val tf = hashingTF.transform(featuresIter)
      val idf = new IDF(config.minTermDocFreq).fit(tf)
      val tfidf = idf.transform(tf)
      
      val featuresWithTfidf = rddFeatures.zip(tfidf)
      
      // map strings or orderd list of ids using broadcast map
      val rddFeatureIds = featuresWithTfidf.map{case ((user,features),tfidfVec) => 
        
        var line = new StringBuilder()
        val hashingTF = new HashingTF()
        var fset = Set[String]()
        for (feature <- features.split(" "))
        {
          if (!fset.contains(feature))
          {
            val id = hashingTF.indexOf(feature)
            val tfidf = tfidfVec(id)
            val field = if (feature.startsWith("u_")) "1" else "2"
            line ++= " "+field+":"+feature+":"+tfidf
            fset += feature
          }
        }
        line.toString().trim()
        }
       
      val outPath = config.outputPath + "/" + config.client + "/features/"+config.startDay
      rddFeatureIds.saveAsTextFile(outPath)
      

      val rddTags = rddFeatures.flatMap{case (user,features) => features.split(" ")}.distinct
      val bcIDF = rddTags.context.broadcast(idf)
      val tagIDFs = rddTags.map { tag =>  
        val idf = bcIDF.value
        val hashingTF = new HashingTF()
        val id = hashingTF.indexOf(tag)
        val vec =  Vectors.sparse(id+1, Seq((id,1.0)))
        val idfVec = idf.transform(vec)
        (tag,idfVec(id))
        }.map{case (tag,idf) => tag+","+idf}
      
      val idfOutPath = config.outputPath + "/" + config.client + "/idf/"+config.startDay
      FileUtils.outputModelToFile(tagIDFs, idfOutPath, DataSourceMode.fromString(idfOutPath), "idf.csv")
    }
    
    
}


object CreateActionFeatures
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[ActionConfig]("CreateActionFeatures") {
    head("CreateActionFeatures", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('c', "client") required() valueName("<client>") action { (x, c) => c.copy(client = x) } text("client name (will be used as db and folder suffix)")    
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey")  valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url (to get dimension for all items)")    
    opt[String]('t', "tagAttr") required() action { (x, c) =>c.copy(tagAttr = x) } text("tag attribute in database")    
    opt[Int]('r', "numdays") required() action { (x, c) =>c.copy(days = x) } text("number of days in past to get actions for")
    opt[Int]("start-day") required() action { (x, c) =>c.copy(startDay = x) } text("start day in unix time")
    opt[Int]("maxNumActionsPerUser") action { (x, c) =>c.copy(maxNumActionsPerUser = x) } text("max number of actions a user must have")
    opt[Int]("actionNumToStart") action { (x, c) =>c.copy(actionNumToStart = x) } text("wait until this number of actions for a user before creating features")

    }
    
    parser.parse(args, ActionConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateActionFeatures")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "13g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      if (config.awsKey.nonEmpty && config.awsSecret.nonEmpty)
      {
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)
      }
      
      val cByd = new CreateActionFeatures(sc,config)
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