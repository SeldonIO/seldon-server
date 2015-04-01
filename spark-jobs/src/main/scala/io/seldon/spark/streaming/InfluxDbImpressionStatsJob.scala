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
package io.seldon.spark.streaming

import kafka.producer._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel


case class ImpressionsStatsConfig(
    local : Boolean = false,
    testing : Boolean = false,
    mini_batch_secs : Int = 30,
    zkQuorum : String = "localhost",
    kafkaGroupId : String = "spark-impressions-group",
    kafka_topics : String = "impressionstopic",
    kafka_numThreadPartitions : Int = 1,
    influxdb_host : String = "",
    influxdb_user : String = "",
    influxdb_pass : String = "",
    influxdb_db : String = "stats",
    influxdb_series : String = "impressions_stream" 
    )

case class Impression(consumer: String, time : Long, imp : Int, click : Int)

class InfluxdbImpressionsStatsJob(private val sc : StreamingContext,config : ImpressionsStatsConfig) {

  def parseJson(lines : org.apache.spark.rdd.RDD[String]) = {
    
    val rdd = lines.flatMap{line =>
      
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      val json = parse(line)
      val tag = (json \ "tag").extract[String]
      if (tag == "restapi.ctr")
      {
        val time = (json \ "time").extract[Long]
        val timeSecs = time / 60
        val consumer = (json \ "consumer").extract[String]
        val click = (json \ "click").extract[String]
        if (click == "IMP")
          Seq((consumer+timeSecs.toString(),(consumer,time,1,0)))
        else
          Seq((consumer+timeSecs.toString(),(consumer,time,0,1)))
      }
      else
        None
      }    
    rdd
  }
  

  def sendStatsToInfluxDb(data : org.apache.spark.rdd.RDD[(String,(String,Long,Int,Int))]) = {
    import org.influxdb.InfluxDBFactory
    import java.util.concurrent.TimeUnit
    
    val rows = data.collect()
    if (rows.length > 0)
    {
      val influxDB = InfluxDBFactory.connect("http://"+config.influxdb_host+":8086", config.influxdb_user, config.influxdb_pass);
      val serie = new org.influxdb.dto.Serie.Builder(config.influxdb_series)
            .columns("time", "client", "impressions", "clicks")
      for(row <- rows)
      {
        val client = row._2._1
        val imps = row._2._3
        val clicks = row._2._4
        val timeSecs = row._2._2
        serie.values(timeSecs : java.lang.Long,client,imps : java.lang.Long,clicks : java.lang.Long)
      }
      influxDB.write(config.influxdb_db, TimeUnit.SECONDS, serie.build());
    }
  }
  
  def run()
  {
    var lines:DStream[String] = null

    if (config.testing)
    {
      println("Running in test mode - will attach to port 7777")
      lines = sc.socketTextStream("localhost", 7777)
    }
    else
    {
      println("Running in Kafka mode")
      val topicMap = config.kafka_topics.split(",").map((_,config.kafka_numThreadPartitions)).toMap
      lines = KafkaUtils.createStream(sc, config.zkQuorum, config.kafkaGroupId, topicMap,StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    }    
      
    lines.foreachRDD((rdd:RDD[String],time : Time) => 
      {
       val stats = parseJson(rdd).reduceByKey{(i1,i2) => (i1._1,Math.max(i1._2,i2._2),i1._3+i2._3,i1._4+i2._4)}
       sendStatsToInfluxDb(stats)
    })
    
    sc.start()
    sc.awaitTermination()
  }
}


object InfluxdbImpressionsStatsJob
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[ImpressionsStatsConfig]("ClusterUsersByDimension") {
    head("InfluxDbImpressionStatsJob", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[Unit]("testing") action { (_, c) => c.copy(testing = true) } text("testing mode - connect to port 7777")    
    opt[String]("influxdb-host") required() valueName("influxdb host") action { (x, c) => c.copy(influxdb_host = x) } text("influx db hostname")    
    opt[String]("influxdb-user") required() valueName("influxdb username") action { (x, c) => c.copy(influxdb_user = x) } text("influx db username")    
    opt[String]("influxdb-pass") required() valueName("influxdb password") action { (x, c) => c.copy(influxdb_pass = x) } text("influx db password")        
    opt[String]("influxdb-db") required() valueName("influxdb db") action { (x, c) => c.copy(influxdb_db = x) } text("influx db database to use")        
    opt[String]("influxdb-series") valueName("influxdb series") action { (x, c) => c.copy(influxdb_series = x) } text("influx db series to add to")            
    opt[String]("zk-quorum") required() valueName("zookeeper nodes") action { (x, c) => c.copy(zkQuorum = x) } text("zookeeper quorum nodes for kafka discovery")            
    opt[String]("kafka-group-id") valueName("kafka group id") action { (x, c) => c.copy(kafkaGroupId = x) } text("kafka group id")                
    opt[String]("kafka-topics") valueName("kafka topics") action { (x, c) => c.copy(kafka_topics = x) } text("kafka topics to subscribe to")                    
    opt[Int]("kafka-thread-partitions") valueName("kafka thread partitions") action { (x, c) => c.copy(kafka_numThreadPartitions = x) } text("kafka number of thread partitions")                        
    opt[Int]("mini-batch-secs") valueName("mini-batch secs") action { (x, c) => c.copy(mini_batch_secs = x) } text("time interval between streaming runs")                            
    }
    
    parser.parse(args, ImpressionsStatsConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("InfluxDbImpressionsStatsJob")
      
    if (config.local)
      conf.setMaster("local[2]")
    
    val sc = new StreamingContext(conf, Seconds(config.mini_batch_secs)) 

    try
    {
      println(config)
      val cByd = new InfluxdbImpressionsStatsJob(sc,config)
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