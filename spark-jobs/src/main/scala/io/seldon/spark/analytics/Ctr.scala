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
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import io.seldon.spark.SparkUtils
import org.apache.spark.sql.SQLContext



case class CtrConfig(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    startDate : String = "",
    endDate : String = "",
    influxdb_host : String = "",
    influxdb_user : String = "root",
    influxdb_pass : String = "")

case class Impression(consumer: String, year: Int, month: Int, day: Int, click : String)
    
class Ctr(private val sc : SparkContext,config : CtrConfig) {

  
  
  def parseJson(lines : org.apache.spark.rdd.RDD[String]) = {

    val rdd = lines.flatMap{line =>
      
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

      
      val parts = line.split("\t")
      val date = formatter.parseDateTime(parts(0))
      if (parts(1) == "restapi.ctr")
      {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      try
      {
        val json = parse(parts(2))
        val consumer = (json \ "consumer").extract[String]
        val click = (json \ "click").extract[String]
        Seq(Impression(consumer,date.getYear(),date.getMonthOfYear(),date.getDayOfMonth(),click))
      }
      catch
      {
        case ex: MappingException => {println("bad json => "+line)}
        None
      }
      }
      else
        None
    }
    
    rdd
  }
  
  def sendStatsToInfluxDb(data : org.apache.spark.sql.SchemaRDD,iHost : String, iUser : String, iPass : String) = {
    import org.influxdb.InfluxDBFactory
    import java.util.concurrent.TimeUnit
    
    val influxDB = InfluxDBFactory.connect("http://"+iHost+":8086", iUser, iPass);
    
    val rows = data.collect()
    val serie = new org.influxdb.dto.Serie.Builder("impressions")
            .columns("time", "client", "impressions", "clicks")
    for(row <- rows)
    {
      val date = new DateTime(row.getInt(1),row.getInt(2),row.getInt(3),0,0)
      val client = row.getString(0)
      val imps = row.getLong(4)
      val clicks = row.getLong(5)
      serie.values(date.getMillis() : java.lang.Long,client,imps : java.lang.Long,clicks : java.lang.Long)
    }
    influxDB.write("stats", TimeUnit.MILLISECONDS, serie.build());
  }
  
  def process(lines : org.apache.spark.rdd.RDD[String],iHost : String, iUser : String, iPass : String) = 
  {
    // sc is an existing SparkContext.
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
       
    val imps = parseJson(lines)
    
    imps.toDF().registerTempTable("imps")
    
    val ctr = sqlContext.sql("select t1.consumer,t1.year,t1.month,t1.day,impressions,clicks,(clicks/impressions)*100 from (select consumer,year,month,day,count(*) as impressions from imps where click='IMP' group by consumer,year,month,day) t1 join (select consumer,year,month,day,count(*) as clicks from imps where click='CTR' group by consumer,year,month,day) t2 on (t1.consumer=t2.consumer and t1.year=t2.year and t1.month=t2.month and t1.day=t2.day)")
    
    if (iHost.nonEmpty)
    {
      sendStatsToInfluxDb(ctr,iHost,iUser,iPass)
    }
    
    ctr
  }
  
  def run()
  {
   
    val startDate = DateTime.parse(config.startDate)
    val endDate = DateTime.parse(config.endDate)
    val glob = config.inputPath + "/" + startDate.getYear() + "/" + SparkUtils.getS3UnixGlob(startDate, endDate)+"/*/*"
    println(glob)

    val lines = sc.textFile(glob)
    
    val ctr = process(lines,config.influxdb_host,config.influxdb_user,config.influxdb_pass)

    val outPath = config.outputPath + "/" + config.startDate+"_"+config.endDate

    ctr.rdd.coalesce(1, false).saveAsTextFile(outPath)
  }
}

object Ctr {
   def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[CtrConfig]("Ctr") {
    head("Ctr", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[String]('d', "start-date") required() valueName("start date") action { (x, c) => c.copy(startDate = x) } text("start date yyyy-mm-dd")
    opt[String]('e', "end-date") required() valueName("end date") action { (x, c) => c.copy(endDate = x) } text("end date yyyy-mm-dd")
    opt[String]("influxdb-host") valueName("influxdb host") action { (x, c) => c.copy(influxdb_host = x) } text("influx db hostname")    
    opt[String]('u', "influxdb-user") valueName("influxdb username") action { (x, c) => c.copy(influxdb_user = x) } text("influx db username")    
    opt[String]('p', "influxdb-pass") valueName("influxdb password") action { (x, c) => c.copy(influxdb_pass = x) } text("influx db password")        
    }
    
    parser.parse(args, CtrConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("CTR")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)
      val cByd = new Ctr(sc,config)
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