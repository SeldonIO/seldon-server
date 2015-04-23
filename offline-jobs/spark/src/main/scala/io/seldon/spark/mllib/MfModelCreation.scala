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
package io.seldon.spark.mllib

import _root_.io.seldon.spark.rdd.FileUtils
import io.seldon.spark.zookeeper.ZkCuratorHandler
import java.io.File
import java.text.SimpleDateFormat
import org.apache.curator.utils.EnsurePath
import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.apache.spark.rdd._
import org.jets3t.service.S3Service
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{S3Object, S3Bucket}
import org.jets3t.service.security.AWSCredentials
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import scala.collection.mutable
import scala.util.Random._
import java.net.URLClassLoader

case class ActionWeightings (actionMapping: List[ActionWeighting])

case class ActionWeighting (actionType:Int = 1,
                             valuePerAction:Double = 1.0,
                             maxSum:Double = 1.0
                             )
case class MfConfig(
    client : String = "",
    inputPath : String = "/seldon-models",
    outputPath : String = "/seldon-models",
    startDay : Int = 1,
    days : Int = 1,    
    awsKey : String = "",
    awsSecret : String = "",
    local : Boolean = false,    
    zkHosts : String = "",
    activate : Boolean = false,
    
    rank : Int = 30,
    lambda : Double = 0.1,
    alpha : Double = 1,
    iterations : Int = 2,
    actionWeightings: Option[List[ActionWeighting]] = None
 )
 

class MfModelCreation(private val sc : SparkContext,config : MfConfig) {

  object DataSourceMode extends Enumeration {
    def fromString(s: String): DataSourceMode = {
      if(s.startsWith("/"))
        return LOCAL
      if(s.startsWith("s3n://"))
        return S3
      return NONE
    }
    type DataSourceMode = Value
    val S3, LOCAL, NONE = Value
  }
  import DataSourceMode._


  def toSparkResource(location:String, mode:DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location
    }

  }

  def toOutputResource(location:String, mode: DataSourceMode): String = {
    mode match {
      case LOCAL => return location.replace("local:/","")
      case S3 => return location.replace("s3n://", "")
    }
  }
  
  def run() 
  {
    val client = config.client
    val date:Int = config.startDay
    val daysOfActions = config.days
    val rank = config.rank
    val lambda = config.lambda
    val alpha = config.alpha
    val iterations = config.iterations
    val zkServer = config.zkHosts
    val inputFilesLocation = config.inputPath + "/" + config.client + "/actions/"
    val inputDataSourceMode = DataSourceMode.fromString(inputFilesLocation)
    if (inputDataSourceMode == NONE) {
      println("input file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val outputFilesLocation = config.outputPath + "/" + config.client +"/matrix-factorization/"
    val outputDataSourceMode = DataSourceMode.fromString(outputFilesLocation)
    if (outputDataSourceMode == NONE) {
      println("output file location must start with local:// or s3n://")
      sys.exit(1)
    }
    val actionWeightings = config.actionWeightings.getOrElse(List(ActionWeighting()))
    // any action not mentioned in the weightings map has a default score of 0.0
    val weightingsMap = actionWeightings.map(
      aw => (aw.actionType, (aw.valuePerAction,aw.maxSum))
    ).toMap.withDefaultValue(.0,.0)

    println("Using weightings map"+weightingsMap)
    val startTime = System.currentTimeMillis()
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // set up environment
    val timeStart = System.currentTimeMillis()
    val glob= toSparkResource(inputFilesLocation, inputDataSourceMode) + ((date - daysOfActions + 1) to date).mkString("{", ",", "}")
    println("Looking at "+glob)
    val actions:RDD[((Int, Int), Int)] = sc.textFile(glob)
      .map { line =>
        val json = parse(line)
        import org.json4s._
        implicit val formats = DefaultFormats
        val user = (json \ "userid").extract[Int]
        val item = (json \ "itemid").extract[Int]
        val actionType = (json \"type").extract[Int]
        ((item,user),actionType)
      }.repartition(2).cache()

    // group actions by user-item key
    val actionsByType:RDD[((Int,Int),List[Int])] = actions.combineByKey((x:Int)=>List[Int](x),
                                                                        (list:List[Int],y:Int)=>y :: list,
                                                                        (l1:List[Int],l2:List[Int])=> l1 ::: l2)
    // count the grouped actions by action type
    val actionsByTypeCount:RDD[((Int,Int),Map[Int,Int])] = actionsByType.map{
      case (key,value:List[Int]) => (key,value.groupBy(identity).mapValues(_.size).map(identity))
    }

    // apply weigtings map
    val actionsByScore:RDD[((Int,Int),Double)] = actionsByTypeCount.mapValues{
      case x:Map[Int,Int]=> x.map {
        case y: (Int, Int) => Math.min(weightingsMap(y._1)._1 * y._2, weightingsMap(y._1)._2)
      }.reduce(_+_)
    }

    actionsByScore.take(10).foreach(println)
    val itemsByCount: RDD[(Int, Int)] = actionsByScore.map(x => (x._1._1, 1)).reduceByKey(_ + _)
    val itemsCount = itemsByCount.count()
    val topQuarter = (itemsCount * 1).toInt
    println("total actions " + actions.count())
    println("total stories " + itemsByCount.count())

    val usersByCount = actionsByScore.map(x=>(x._2,1)).reduceByKey(_+_)
    val usersCount = usersByCount.count()
    println("total users " + usersCount)

    val ratings = actionsByScore.map{
      case ((product, user), rating) => Rating(user,product,rating)
    }.repartition(2).cache()

    val timeFirst = System.currentTimeMillis()
    println("munging data took "+(timeFirst-timeStart)+"ms")
    val model: MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, iterations, lambda, alpha)
    println("training model took "+(System.currentTimeMillis() - timeFirst)+"ms")
    outputModelToFile(model, toOutputResource(outputFilesLocation,outputDataSourceMode), outputDataSourceMode, client,date)

    if (config.activate)
    {
      val curator = new ZkCuratorHandler(zkServer)
      if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
      {
        val zkPath = "/all_clients/"+client+"/mf"
        val ensurePath = new EnsurePath(zkPath)
        ensurePath.ensure(curator.getCurator.getZookeeperClient)
        curator.getCurator.setData().forPath(zkPath,(outputFilesLocation+date).getBytes())
      }
    }
    
    println(List(rank,lambda,iterations,alpha,0,System.currentTimeMillis() - timeFirst).mkString(","))

    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
    val rdds = sc.getRDDStorageInfo
    if(sc.getPersistentRDDs !=null) 
    {
        for (rdd <- sc.getPersistentRDDs.values) {
          if (rdd.name != null && (rdd.name.startsWith("product") || rdd.name.startsWith("user"))) {
            rdd.unpersist(true);
          }
        }
    }

    sc.stop()
    println("Time taken " + (System.currentTimeMillis() - startTime))
  }

  def outputModelToLocalFile(model: MatrixFactorizationModel, outputFilesLocation: String, yesterdayUnix: Long) = {
    new File(outputFilesLocation+yesterdayUnix).mkdirs()
    val userFile = new File(outputFilesLocation+yesterdayUnix+"/userFeatures.txt");
    userFile.createNewFile()

    val productFile = new File(outputFilesLocation+yesterdayUnix+"/productFeatures.txt");
    productFile.createNewFile()

    outputToFile(model, userFile, productFile)
    val gzipUserFile:File = FileUtils.gzip(userFile.getAbsolutePath)
    println(gzipUserFile.getAbsolutePath)
    val gzipProdFile = FileUtils.gzip(productFile.getAbsolutePath)
  }

  def outputToFile(model: MatrixFactorizationModel, userFile: File, prodFile: File): Unit = {
    printToFile(userFile) {
      p => model.userFeatures.collect().foreach {
        u => {
          val strings = u._2.map(d => f"$d%.5g")
          p.println(u._1.toString + "|" + strings.mkString(","))
        }
      }
    }
    printToFile(prodFile) {
      p => model.productFeatures.collect().foreach {
        u => {
          val strings = u._2.map(d => f"$d%.5g")
          p.println(u._1.toString + "|" + strings.mkString(","))
        }

      }
    }

  }

  def outputModelToS3File(model: MatrixFactorizationModel, outputFilesLocation: String, yesterdayUnix: Long) = {
    // write to temp file first
    val tmpUserFile = File.createTempFile("mfmodelUser",".tmp");
    tmpUserFile.deleteOnExit()
    val tmpProdFile = File.createTempFile("mfModelProduct",".tmp")
    tmpProdFile.deleteOnExit()
    outputToFile(model, tmpUserFile, tmpProdFile)
    val gzipUserFile:File = FileUtils.gzip(tmpUserFile.getAbsolutePath)
    println(gzipUserFile.getAbsolutePath)
    val gzipProdFile = FileUtils.gzip(tmpProdFile.getAbsolutePath)
    val service: S3Service = new RestS3Service(new AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))
    val bucketString = outputFilesLocation.split("/")(0)
    val bucket = service.getBucket(bucketString)
    val s3Folder = outputFilesLocation.replace(bucketString+"/","")

    val obj = new S3Object(tmpUserFile)
    obj.setKey(s3Folder+yesterdayUnix+"/userFeatures.txt")
    service.putObject(bucket, obj)
    val objZip = new S3Object(gzipUserFile)
    objZip.setKey(s3Folder+yesterdayUnix+"/userFeatures.txt.gz")
    service.putObject(bucket, objZip)
    System.out.println("Uploading user features to " + bucketString + " bucket " + obj.getKey + " file")
    val objProd = new S3Object(tmpProdFile)
    objProd.setKey(s3Folder+yesterdayUnix+"/productFeatures.txt")
    service.putObject(bucket, objProd)
    val objProdZip = new S3Object(gzipProdFile)
    objProdZip.setKey(s3Folder+yesterdayUnix+"/productFeatures.txt.gz")
    service.putObject(bucket, objProdZip)
    System.out.println("Uploading product features to " + bucketString + " bucket " + objProd.getKey + " file")
  }

  def outputModelToFile(model: MatrixFactorizationModel,outputFilesLocation:String, outputType:DataSourceMode, client:String, yesterdayUnix: Long) {
    outputType match {
      case LOCAL => outputModelToLocalFile(model,outputFilesLocation,  yesterdayUnix)
      case S3 => outputModelToS3File(model, outputFilesLocation, yesterdayUnix)
    }


  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}

/**
 * @author firemanphil
 *         Date: 14/10/2014
 *         Time: 15:16
 *
 */
object MfModelCreation {

 
  def updateConf(config : MfConfig) =
  {
    var c = config.copy()
    if (config.zkHosts.nonEmpty) 
     {
       val curator = new ZkCuratorHandler(config.zkHosts)
       val path = "/all_clients/"+config.client+"/offline/matrix-factorization"
       if (curator.getCurator.checkExists().forPath(path) != null)
       {
         val bytes = curator.getCurator.getData().forPath(path)
         val j = new String(bytes,"UTF-8")
         println("Configuration from zookeeper -> ",j)
         import org.json4s._
         import org.json4s.jackson.JsonMethods._
         implicit val formats = DefaultFormats
         val json = parse(j)
         import org.json4s.JsonDSL._
         import org.json4s.jackson.Serialization.write
         type DslConversion = MfConfig => JValue
         val existingConf = write(c) // turn existing conf into json
         val existingParsed = parse(existingConf) // parse it back into json4s internal format
         val combined = existingParsed merge json // merge with zookeeper value
         c = combined.extract[MfConfig] // extract case class from merged json
         c
       }
       else 
       {
           println("Warning: using default configuration - path["+path+"] not found!");
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
    
    var c = new MfConfig()
    val parser = new scopt.OptionParser[Unit]("MatrixFactorization") {
    head("ClusterUsersByDimension", "1.x")
        opt[Unit]('l', "local") foreach { x => c = c.copy(local = true) } text("local mode - use local Master")
        opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
        opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
        opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
        opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
        opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
        opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
        opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
        opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")        
        opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")
        
        opt[Int]('u', "rank") foreach { x =>c = c.copy(rank = x) } text("the number of latent factors in the model")
        opt[Double]('m', "lambda") foreach { x =>c = c.copy(lambda = x) } text("the regularization parameter in ALS to stop over-fitting")
        opt[Double]('m', "alpha") foreach { x =>c = c.copy(alpha = x) } text("governs the baseline confidence in preference observations")        
        opt[Int]('u', "iterations") foreach { x =>c = c.copy(iterations = x) } text("the number of iterations to run the modelling")
    }
    
    
    if (parser.parse(args)) // Parse to check and get zookeeper if there
    {
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line
      
      val conf = new SparkConf().setAppName("MatrixFactorization")

      if (c.local)
        conf.setMaster("local")
        .set("spark.executor.memory", "8g")

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
        val mf = new MfModelCreation(sc,c)
        mf.run()
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
