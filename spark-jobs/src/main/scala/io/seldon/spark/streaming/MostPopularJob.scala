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

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.json4s._
import org.json4s.jackson.JsonMethods._
/**
 * @author firemanphil
 *         Date: 05/11/14
 *         Time: 15:40
 *
 */

case class MostPopularConfig(
    local : Boolean = false,
    testing : Boolean = false,
    clients : String = "",
    jdbc : String = "",
    mini_batch_secs : Int = 30,
    zkQuorum : String = "localhost",
    kafkaGroupId : String = "spark-impressions-group",
    kafka_topics : String = "impressionstopic",
    kafka_numThreadPartitions : Int = 1
    )




class MostPopularJob(private val sc : StreamingContext,config : MostPopularConfig) {


  
 

  def run()
  {
    var lines:DStream[String] = null
    val connectionString:String = config.jdbc
    if (config.testing){
      lines = sc.socketTextStream("localhost", 7777)
    } else 
    {
      val topicMap = config.kafka_topics.split(",").map((_,config.kafka_numThreadPartitions)).toMap

      lines = KafkaUtils.createStream(sc, config.zkQuorum, config.kafkaGroupId, topicMap).map(_._2)
    }

    val okClients= config.clients
    val linesMapped = lines
      .map{
      case line =>
        val json = parse(line)
        import org.json4s._
        implicit val formats = DefaultFormats
        val client = (json \ "client").extract[String]
        val item = Integer.parseInt((json \ "itemid").extract[String])
        ((client, item),1)
    } .filter(x => okClients.contains(x._1._1))
    val counts = linesMapped
      .reduceByKey(_+_)
    val countsRemapped = counts.map(x=> (x._1._1,(x._1._2, x._2))).repartition(2)
    countsRemapped.foreachRDD(x => {
      
      x.groupBy(_._1).foreach(x=>MostPopularJob.updateDb(x._1, x._2, connectionString))
    })




    sc.start()
    sc.awaitTermination()
  }


}


object MostPopularJob
{
   def updateDb(client: String, records: Iterable[(String, (Int, Int))], conn_str: String) {
import java.sql.{ResultSet, DriverManager}
    classOf[com.mysql.jdbc.Driver]
    val dbclient:String = client 
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try {
    // Configure to be Read Only
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    var query: StringBuilder = new StringBuilder

    query = query.append("insert into "+dbclient+".items_recent_popularity (item_id, score, decay_id) values ")
    query =
      ((records.tail foldLeft (query.append("("+ records.head._2._1 + "," + records.head._2._2+",1)"))) {(acc, e) => acc.append(", ").append("("+ e._2._1 + "," + e._2._2+",1)")})

    query = query.append(" on duplicate key update score=VALUES(score)+exp(-(greatest(CURRENT_TIMESTAMP()-last_update,0)/82400))*score")
    println(query)
    // Execute Query
    statement.execute(query.toString())

  }
    finally {
    conn.close
  }
  }
  
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[MostPopularConfig]("MostPopularJob") {
    head("InfluxDbImpressionStatsJob", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[Unit]("testing") action { (_, c) => c.copy(testing = true) } text("testing mode - connect to port 7777")    
    opt[String]("zk-quorum") valueName("zookeeper nodes") action { (x, c) => c.copy(zkQuorum = x) } text("zookeeper quorum nodes for kafka discovery")            
    opt[String]("kafka-group-id") valueName("kafka group id") action { (x, c) => c.copy(kafkaGroupId = x) } text("kafka group id")                
    opt[String]("kafka-topics") valueName("kafka topics") action { (x, c) => c.copy(kafka_topics = x) } text("kafka topics to subscribe to")                    
    opt[String]("clients") required() valueName("clients") action { (x, c) => c.copy(clients = x) } text("clients to process")                    
    opt[Int]("kafka-thread-partitions") valueName("kafka thread partitions") action { (x, c) => c.copy(kafka_numThreadPartitions = x) } text("kafka number of thread partitions")                        
    opt[Int]("mini-batch-secs") valueName("mini-batch secs") action { (x, c) => c.copy(mini_batch_secs = x) } text("time interval between streaming runs")         
    opt[String]('j', "jdbc") required() valueName("<JDBC URL>") action { (x, c) => c.copy(jdbc = x) } text("jdbc url")    
    }
    
    parser.parse(args, MostPopularConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("InfluxDbImpressionsStatsJob")
    
    if (config.local)
      conf.setMaster("local[2]")
    
    val sc = new StreamingContext(conf, Seconds(config.mini_batch_secs)) 
    
    try
    {
      println(config)
      val cByd = new MostPopularJob(sc,config)
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
