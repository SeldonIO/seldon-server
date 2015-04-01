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
package io.seldon.spark.analytics

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import io.seldon.spark.SparkUtils
import scala.collection.mutable.ListBuffer

case class EngagementConfig(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    startDate : String = "",
    endDate : String = "",
    maxIntraSessionGapSecs : Int = 600,
    maxSessionTimeSecs : Int = 1800,
    maxSessionPageView : Int = 50)

case class EngImpression(consumer: String, time: Long, user : String,abkey : String)
    
class Engagement(private val sc : SparkContext,config : EngagementConfig) {

  
  
  def parseJson(path : String) = {
    
    val rdd = sc.textFile(path).flatMap{line =>
      
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

      
      val parts = line.split("\t")
      val date = formatter.parseDateTime(parts(0))
      if (parts(1) == "restapi.ctralg")
      {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      
      val json = parse(parts(2))
      val consumer = (json \ "consumer").extract[String]
      val click = (json \ "click").extract[String]
      val user = (json \ "userid").extract[String]
      val abkey = (json \ "abkey").extract[String]
      if (click == "IMP")
      {
        Seq((consumer+"_"+user,EngImpression(consumer,date.getMillis(),user,abkey)))
      }
      else
        None
      }
      else
        None
    }
    
    rdd
  }
  
  def run()
  {
   
    val startDate = DateTime.parse(config.startDate)
    val endDate = DateTime.parse(config.endDate)
    val glob = config.inputPath + "/" + startDate.getYear() + "/" + SparkUtils.getS3UnixGlob(startDate, endDate)+"/*/*"
    println(glob)
    val maxGapMsecs = config.maxIntraSessionGapSecs * 1000
    val maxSessionPageViews = config.maxSessionPageView
    val data = parseJson(glob)

    // calulate session time and number of page views per session for each user page view history
    val perUserStats = data.groupByKey(50).flatMapValues{v => 
      val buf = new ListBuffer[(Long,Long,String,String,Int,Int,Int)]()
      val sorted = v.toArray.sortBy(_.time)
      var lastTime : Long = 0
      var timeSecs : Long = 0
      var pv = 0
      var abkey = sorted(0).abkey
      val client = sorted(0).consumer
      var numSessions = 0
      for(e <- sorted)
      {
        if (lastTime > 0)
        {
          val gap = (e.time - lastTime)
          if (gap > maxGapMsecs)
          {
            if (timeSecs == 0){ timeSecs = 1}
            if (pv <= maxSessionPageViews)
            {
              numSessions += 1
              var userCount = 0
              var pv1Plus = 0
              if (numSessions == 1) {userCount = 1}
              if (pv > 1) { pv1Plus = 1}
              buf.append((timeSecs,pv,abkey,client,userCount,1,pv1Plus))
            }
            timeSecs = 0
            pv = 0
          }
          else
          {
            timeSecs += (gap/1000)
          }
        }
        lastTime = e.time
        pv += 1
        abkey = e.abkey // user can change group they are in after a certain time
      }
      if (pv > 0)
      {
        if (timeSecs == 0){ timeSecs = 1}
        if (pv <= maxSessionPageViews)
        {
          numSessions += 1
          var userCount = 0
          var pv1Plus = 0
          if (numSessions == 1) {userCount = 1}
          if (pv > 1) { pv1Plus = 1}
          buf.append((timeSecs,pv,abkey,client,userCount,1,pv1Plus))
        }
      }
      buf
      }
    
    // create new per client key
    val stats = perUserStats.map{case (key,(time,pv,abkey,client,userCount,sessionCount,pv1plusCount)) => (client+"_"+abkey,(time,pv,userCount,sessionCount,pv1plusCount))}
      
    // get sums and counts
    val stats2 = stats.reduceByKey{case ((time1,pv1,userCount1,sessionCount1,pv1p1),(time2,pv2,userCount2,sessionCount2,pv1p2)) => (time1+time2,pv1+pv2,userCount1+userCount2,sessionCount1+sessionCount2,pv1p1+pv1p2)}
    
    // calculate averages
    val stats3 = stats2.mapValues{case (timeSum,pvSum,userCount,sessionCount,pv1plusCount) => (1.0*timeSum/sessionCount,1.0*pvSum/sessionCount,1.0*pv1plusCount/sessionCount,timeSum,pvSum,userCount,sessionCount,pv1plusCount)}
    
    val outPath = config.outputPath + "/" + config.startDate+"_"+config.endDate
    stats3.coalesce(1, true).saveAsTextFile(outPath)
  }
}

object Engagement {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[EngagementConfig]("Ctr") {
    head("Ctr", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('d', "start-date") required() valueName("start date") action { (x, c) => c.copy(startDate = x) } text("start date yyyy-mm-dd")
    opt[String]('e', "end-date") required() valueName("end date") action { (x, c) => c.copy(endDate = x) } text("end date yyyy-mm-dd")
    opt[Int]('m', "max-session-pv") valueName("max session page views") action { (x, c) => c.copy(maxSessionPageView = x) } text("max session page views")
    opt[Int]('g', "max-intra-session-gap-secs") valueName("max intra session gap secs") action { (x, c) => c.copy(maxIntraSessionGapSecs = x) } text("max intra session gap secs")
        }
    
    parser.parse(args, EngagementConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CreateVWTopicTraining")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)
      val cByd = new Engagement(sc,config)
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