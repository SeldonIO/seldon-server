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
package io.seldon.spark.features

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd._
import java.io.FileOutputStream
import java.io.ObjectOutputStream

case class QueryVectorsConfig (
    local : Boolean = false,
    inputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    query : String = "",
    outputPath : String = "")

class QueryVectors(private val sc : SparkContext,config : QueryVectorsConfig) {

  def loadVectors(inputPath : String) : org.apache.spark.rdd.RDD[(String,Array[Float])] =
  {
    sc.textFile(inputPath).map{line =>
      val arr = line.split(",")
      val key = arr(0)
      val scores = arr.slice(1, arr.length).map(_.toFloat)
      (key,scores)
    }
  }
  
  def run()
  {
   val vectors = loadVectors(config.inputPath).collectAsMap()
   
   val model = new Word2VecModel(collection.immutable.HashMap(vectors.toSeq:_*))
   
   val synonyms = model.findSynonyms(config.query, 50)
   
   for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
  }
   
  }
 
}


object QueryVectors
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[QueryVectorsConfig]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-path") required() valueName("input path") action { (x, c) => c.copy(inputPath = x) } text("input location with sentences")
    opt[String]('i', "query") required() valueName("query") action { (x, c) => c.copy(query = x) } text("query to find synonyms")    
//    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    
    }
    
    parser.parse(args, QueryVectorsConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("FindShortestPath")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    .set("spark.akka.frameSize", "300")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)

      val cByd = new QueryVectors(sc,config)
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