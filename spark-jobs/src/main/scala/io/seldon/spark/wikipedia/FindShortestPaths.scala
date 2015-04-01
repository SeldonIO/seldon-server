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
package io.seldon.spark.wikipedia

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.TriangleCountEdge
import org.apache.spark.graphx.VertexRDD
import org.apache.commons.lang.StringEscapeUtils
import scala.collection.mutable.ListBuffer
import org.apache.spark.graphx.util.GraphGenerators
import org.jets3t.service.S3Service
import scala.collection.mutable.ArrayBuffer
 

case class PathConfig (
    local : Boolean = false,
    graphInputPath : String = "",
    outputPath : String = "",
    query : String = "",
    queryInputPath : String = "",
    targets : String = "",
    targetsInputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    maxVertexTriangles : Int = Int.MaxValue,
    numResultsPerQuery : Int = 100)

class FindShortestPaths(private val sc : SparkContext,config : PathConfig) {

  /*
  def outputResultsToFile(results : String) {
    import org.jets3t.service.impl.rest.httpclient.RestS3Service
    import org.jets3t.service.model.{S3Object, S3Bucket}
    import org.jets3t.service.security.AWSCredentials

    val service: S3Service = new RestS3Service(new AWSCredentials(config.awsKey, config.awsSecret))
    val bucket = service.getBucket("seldon-data")

    val obj = new S3Object(config.outputPath+"/tag_expansion.txt", results)
    service.putObject(bucket, obj)
  }
  */
  
  def outputResultsToFile(results : Array[String]) {
    val rdd = sc.parallelize(results, 1)
    rdd.saveAsTextFile(config.outputPath)
  }
  
  def doSearch(maxTriangles : Int,initialGraph : org.apache.spark.graphx.Graph[(Double,Int,Int),Double]) : org.apache.spark.graphx.Graph[(Double,Int,Int),Double] =
  {
    val sssp = initialGraph.pregel((Double.PositiveInfinity,0,0))(
      (id, attr1, attr2) => {
        if (attr1._1 < attr2._1)
          attr1
      else
          attr2
      },
      triplet => {  // Send Message
        if (triplet.srcAttr._2 < 4 && (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1) && triplet.dstAttr._3 < maxTriangles) {

          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr,triplet.srcAttr._2+1,triplet.dstAttr._3)))
        } else {
          Iterator.empty
      }
      },
      (a,b) => {
        if (a._1 < b._1)
        a
        else
        b
      }
      )
      sssp
  }
  
  def run()
  {
   
    val vertices = sc.textFile(config.graphInputPath+"/vertices").flatMap{line => 
        val parts = line.substring(1,line.length()-1).split(",")
        if (parts.length < 3)
          None
        else
        {
          val nameNumTri = parts(1)+","+parts(2)
          val parts2 = nameNumTri.substring(1,nameNumTri.length()-1).split(",")
          Seq((parts(0).toLong,(parts2(0),parts2(1).toInt)))
        }
      }.cache()
    
    val edges = sc.textFile(config.graphInputPath+"/edges").map{line => 
        val parts = line.substring(5,line.length()-1).split(",")
        val from = parts(0).toLong
        val to = parts(1).toLong
        val weight = parts(2).toDouble
        Edge(parts(0).toLong,parts(1).toLong,1.0-parts(2).toDouble)
      }

    
    var queries : Array[String] = null
    if (config.query.nonEmpty)
      queries = config.query.split(",").map(_.toLowerCase())
    else if (config.queryInputPath.nonEmpty)
      queries = sc.textFile(config.queryInputPath, 1).map(_.toLowerCase()).collect()


    var targets : Array[String] = null
    if (config.targets.nonEmpty)
      targets = config.targets.split(",")
    else if (config.targetsInputPath.nonEmpty)
      targets = sc.textFile(config.targetsInputPath, 1).collect()
    else
      targets = Array.empty[String]
    
    val queryToVertexId = vertices.filter( v => queries.contains(v._2._1) || targets.contains(v._2._1)).map(t => (t._2._1,t._1)).collectAsMap()
    println("map size-->"+queryToVertexId.size)
    val vMap = vertices.collectAsMap()
    val graph = Graph(vertices,edges)
    var maxSearchDepth = 4
    if (targets.nonEmpty)
      maxSearchDepth = 4
    else
      maxSearchDepth = 2

    val results = ArrayBuffer[String]()
    var counter : Int = 0
    println("Running "+queries.size+" queries")

    for (query <- queries)
    {
      counter = counter + 1
      println("looking at query "+counter.toString()+" "+query)
      if (queryToVertexId.contains(query))
      {
        println("Found "+query)
      val sourceId: Long = queryToVertexId(query)
      // Initialize the graph such that all vertices except the root have distance infinity.
      val initialGraph = graph.mapVertices((id, nameNumTri) => if (id == sourceId) (0.0,0,nameNumTri._2) else (100000.0,0,nameNumTri._2))
      val sssp = doSearch(config.maxVertexTriangles, initialGraph)
  
      if (targets.nonEmpty)
      {
        val targetIds = queryToVertexId.values.toSet
        val matches = sssp.vertices.filter( v => targetIds.contains(v._1)).collectAsMap()
        for ((id,weight) <- matches)
          results.append(query+","+vMap(id)._1+","+weight)
      }
      else
      {
        val numResults = config.numResultsPerQuery
        val matches = sssp.vertices.filter(_._2._1 < 10).sortBy(_._2._1, true, 1).take(numResults)

        for ((id,weight) <- matches)
          results.append(query+","+vMap(id)._1+","+weight)
      }
      }
      else
        println("Not found "+query)
    }
    outputResultsToFile(results.toArray)
  }
 
}


object FindShortestPaths
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[PathConfig]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-graph-path") required() valueName("input graph path") action { (x, c) => c.copy(graphInputPath = x) } text("path for graph input needs subfolders vertices and edges")
    opt[String]('i', "query-input-path")  valueName("search terms input file") action { (x, c) => c.copy(queryInputPath = x) } text("file with query words to start search from. Comma separated.")
    opt[String]('i', "targets-input-path") valueName("target terms input file") action { (x, c) => c.copy(targetsInputPath = x) } text("file with target words to score. Comma separated.")
    opt[String]('i', "query") valueName("search terms") action { (x, c) => c.copy(query = x) } text("query words to start search from. Comma separated.")
    opt[String]('i', "targets") valueName("target terms") action { (x, c) => c.copy(targets = x) } text("target words to score. Comma separated.")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('v', "max-vertex-triangles") valueName("max vertex triangles") action { (x, c) => c.copy(maxVertexTriangles = x) } text("max vertex triangles in search")
    opt[Int]('n', "num-results-per-query") valueName("num results per query") action { (x, c) => c.copy(numResultsPerQuery = x) } text("number of results per query")    
    }
    
    parser.parse(args, PathConfig()) map { config =>
    val conf = new SparkConf()
      .setAppName("FindShortestPath")
      
    if (config.local)
      conf.setMaster("local")
    .set("spark.executor.memory", "8g")
    
    val sc = new SparkContext(conf)
    try
    {
      sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)

      val cByd = new FindShortestPaths(sc,config)
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