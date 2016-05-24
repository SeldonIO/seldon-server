package io.seldon.spark.streaming

/**
 * @author clive
 */
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


case class APIStatsConfig(
    local : Boolean = false,
    testing : Boolean = false,
    mini_batch_secs : Int = 30,
    zkQuorum : String = "localhost",
    kafkaGroupId : String = "spark-impressions-group",
    kafka_topics : String = "impressionstopic",
    kafka_numThreadPartitions : Int = 1,
    influxdb_host : String = "",
    influxdb_port : Int = 8086,
    influxdb_user : String = "",
    influxdb_pass : String = "",
    influxdb_db : String = "seldon",
    influxdb_measurement : String = "requests" 
    )

case class Request(consumer: String, time : Long, httpmethod : String, path : String, exectime : Int, count : Int)

class InfluxdbAPIStatsJob(private val sc : StreamingContext,config : APIStatsConfig) {

  def parseJson(lines : org.apache.spark.rdd.RDD[String]) = {
    
    val rdd = lines.flatMap{line =>
      
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats = DefaultFormats
      println(line)
      val json = parse(line)
      val tag = (json \ "tag").extract[String]
      if (tag == "restapi.calls")
      {
        val time = (json \ "time").extract[Long]
        val timeSecs = time / 60
        val consumer = (json \ "consumer").extract[String]
        var path = (json \ "path").extract[String]
        var exectimeStr = (json \ "exectime").extract[String]
        val exectime = exectimeStr.toInt
        var httpmethod = (json \ "httpmethod").extract[String]
        Seq((consumer+"_"+httpmethod+"_"+path+"_"+timeSecs.toString(),Request(consumer,time,httpmethod,path,exectime,1)))
      }
      else
        None
      }    
    rdd
  }
  

  def sendStatsToInfluxDb(data : org.apache.spark.rdd.RDD[(String,Request)]) = {
    import org.influxdb.InfluxDBFactory
    import org.influxdb.dto.Point
    import org.influxdb.dto.BatchPoints
    import java.util.concurrent.TimeUnit
    
    val rows = data.collect()
    if (rows.length > 0)
    {
      val influxDB = InfluxDBFactory.connect("http://"+config.influxdb_host+":"+config.influxdb_port, config.influxdb_user, config.influxdb_pass);
      
      val batchPoints = BatchPoints
                .database(config.influxdb_db)
                .tag("async", "true")
                .retentionPolicy("default")
                .build();
      
      //val serie = new org.influxdb.dto.Serie.Builder(config.influxdb_series)
      //      .columns("time", "client", "impressions", "clicks")
      for(row <- rows)
      {
        val point1 = Point.measurement(config.influxdb_measurement)
                    .time(row._2.time, TimeUnit.SECONDS)
                    .tag("client", row._2.consumer)
                    .tag("httpmethod", row._2.httpmethod)
                    .tag("path", row._2.path)
                    .addField("exectime", row._2.exectime/row._2.count.toFloat)
                    .addField("count", row._2.count)
                    .build();
      batchPoints.point(point1);
      
        //serie.values(timeSecs : java.lang.Long,client,imps : java.lang.Long,clicks : java.lang.Long)
      }
      println("Number of points "+batchPoints.getPoints.size())
      influxDB.write(batchPoints);
      
      //influxDB.write(config.influxdb_db, TimeUnit.SECONDS, serie.build());
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
       val stats = parseJson(rdd).reduceByKey{(i1,i2) => (Request(i1.consumer,Math.max(i1.time,i2.time),i1.httpmethod,i1.path,i1.exectime+i2.exectime,i1.count+i2.count))}
       sendStatsToInfluxDb(stats)
    })
    
    sc.start()
    sc.awaitTermination()
  }
}


object InfluxdbAPIStatsJob
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[APIStatsConfig]("ClusterUsersByDimension") {
    head("InfluxdbAPIStatsJob", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[Unit]("testing") action { (_, c) => c.copy(testing = true) } text("testing mode - connect to port 7777")    
    opt[String]("influxdb-host") required() valueName("influxdb host") action { (x, c) => c.copy(influxdb_host = x) } text("influx db hostname")    
    opt[Int]("influxdb-port") required() valueName("influxdb port") action { (x, c) => c.copy(influxdb_port = x) } text("influx db port")    
    opt[String]("influxdb-user") required() valueName("influxdb username") action { (x, c) => c.copy(influxdb_user = x) } text("influx db username")    
    opt[String]("influxdb-pass") required() valueName("influxdb password") action { (x, c) => c.copy(influxdb_pass = x) } text("influx db password")        
    opt[String]("influxdb-db") required() valueName("influxdb db") action { (x, c) => c.copy(influxdb_db = x) } text("influx db database to use")        
    opt[String]("influxdb-measurement") valueName("influxdb series") action { (x, c) => c.copy(influxdb_measurement = x) } text("influx db series to add to")            
    opt[String]("zk-quorum") required() valueName("zookeeper nodes") action { (x, c) => c.copy(zkQuorum = x) } text("zookeeper quorum nodes for kafka discovery")            
    opt[String]("kafka-group-id") valueName("kafka group id") action { (x, c) => c.copy(kafkaGroupId = x) } text("kafka group id")                
    opt[String]("kafka-topics") valueName("kafka topics") action { (x, c) => c.copy(kafka_topics = x) } text("kafka topics to subscribe to")                    
    opt[Int]("kafka-thread-partitions") valueName("kafka thread partitions") action { (x, c) => c.copy(kafka_numThreadPartitions = x) } text("kafka number of thread partitions")                        
    opt[Int]("mini-batch-secs") valueName("mini-batch secs") action { (x, c) => c.copy(mini_batch_secs = x) } text("time interval between streaming runs")                            
    }
    
    parser.parse(args, APIStatsConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("InfluxdbAPIStats Job")
      
    if (config.local)
      conf.setMaster("local[2]")
    
    val sc = new StreamingContext(conf, Seconds(config.mini_batch_secs)) 

    try
    {
      println(config)
      val cByd = new InfluxdbAPIStatsJob(sc,config)
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