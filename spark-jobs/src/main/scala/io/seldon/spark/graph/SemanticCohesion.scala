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
package io.seldon.spark.graph

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
import org.apache.spark.graphx.GraphLoader

case class Config(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    minNumTrianglesEdge : Int = 1)

case class SemanticAttrs(name : Int,triangles : Map[Long,Int],numTriangles : Int,degrees : Int,clusterCoef : Float)
    
class SemanticCohesion(private val sc : SparkContext,config : Config) {

  def run()
  {
    // create graph from vertices and edges
    val graph = GraphLoader.edgeListFile(sc,config.inputPath)
    
    // do traingle counting on graph removing traingles with low support
    val triCounts = TriangleCountEdge.run(graph,config.minNumTrianglesEdge).vertices.filter(_._2._1.size > 0)
    
    // join to original graph the triangle result
    val graph2 = graph.outerJoinVertices(triCounts){(id,oldAttr,tri) =>
      tri match {
      case Some((mapOfTriangles,numTriangles)) => (oldAttr,mapOfTriangles,numTriangles)
      case None => (oldAttr,Map(id->0),0)
      }
    }
    
    val degrees: VertexRDD[Int] = graph.degrees
    
    // calc cluster coefficient
    val graph3 = graph2.outerJoinVertices(degrees){(id,oldAttr,degOpt) =>
      val (name,mapOfTriangles,numOfTriangles) = oldAttr
      degOpt match {
      case Some(deg) => if (deg>1){SemanticAttrs(name,mapOfTriangles,numOfTriangles,deg,2*numOfTriangles/(deg*(deg-1)).floatValue())}else{SemanticAttrs(name,mapOfTriangles,numOfTriangles,0,0)}
      case None => SemanticAttrs(name,mapOfTriangles,numOfTriangles,0,0)
      }
    }
    
    val semEdges = graph3.vertices.flatMap{v => 
      val map = v._2.triangles
      val buf = new ListBuffer[Edge[Float]]()
      for ((vid,triangles) <- map)
      {
        if (v._2.numTriangles > 0)
        {
          //buf.append(Edge(v._1,vid,triangles.toFloat/(v._2.numTriangles.toFloat * 2.0F) * v._2.clusterCoef))
          buf.append(Edge(v._1,vid,triangles))
        }
      }
      buf
      }
    
    val semVertices = graph3.mapVertices((vid,vd) => (vd.name,vd.numTriangles)).vertices.filter(_._2._2 > 0);
    
    val graph4 = Graph(semVertices,semEdges)
    
    graph4.edges.saveAsTextFile(config.outputPath+"/edges")
    graph4.vertices.saveAsTextFile(config.outputPath+"/vertices")
    //graph3.vertices.saveAsTextFile(config.outputPath+"/graph")
    
  }
  
}

object SemanticCohesion
{
  def main(args: Array[String]) 
  {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val parser = new scopt.OptionParser[Config]("ClusterUsersByDimension") {
    head("CrateVWTopicTraining", "1.x")
    opt[Unit]('l', "local") action { (_, c) => c.copy(local = true) } text("debug mode - use local Master")
    opt[String]('i', "input-path") required() valueName("path url") action { (x, c) => c.copy(inputPath = x) } text("path prefix for input")
    opt[String]('o', "output-path") required() valueName("path url") action { (x, c) => c.copy(outputPath = x) } text("path prefix for output")
    opt[String]('a', "awskey") required() valueName("aws access key") action { (x, c) => c.copy(awsKey = x) } text("aws key")
    opt[String]('s', "awssecret") required() valueName("aws secret") action { (x, c) => c.copy(awsSecret = x) } text("aws secret")
    opt[Int]('m', "min-triangles") required() valueName("min number triangles") action { (x, c) => c.copy(minNumTrianglesEdge = x) } text("min triangles")    
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
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsKey)
      sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecret)

      val cByd = new SemanticCohesion(sc,config)
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