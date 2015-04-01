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

case class Config(
    local : Boolean = false,
    inputPath : String = "",
    outputPath : String = "",
    awsKey : String = "",
    awsSecret : String = "",
    minNumTrianglesEdge : Int = 2)

case class SemanticAttrs(name : String,triangles : Map[Long,Int],numTriangles : Int,degrees : Int,clusterCoef : Float)
    
class SemanticCohesion(private val sc : SparkContext,config : Config) {

  def run()
  {
    // get page1 -< page2 links
    val rddPages = sc.textFile(config.inputPath).flatMap{line => 
      if (line.startsWith("#"))
      {
        None
      }
      else
      {
        val p = line.split(" ") 
        val page1 = p(0).substring(32, p(0).length()-1)
        val page2 = p(2).substring(32, p(2).length()-1)
        if (!(page1.forall(_.isDigit)) && !(page2.forall(_.isDigit)) && !page1.startsWith("file:") && !page2.startsWith("file:") && !page1.contains(",") && !page2.contains(","))
          Seq((StringEscapeUtils.unescapeJava(page1)),(StringEscapeUtils.unescapeJava(page2)))
        else
          None
      }
    }
    
    // create unique ids for each page
    val pageIds = rddPages.distinct().zipWithUniqueId().cache()
    
    val titleToId = pageIds.collectAsMap
    
    val broadcastMap = sc.broadcast(titleToId)
    
    // transform pages to edges in graph outputing only edges of form id1->id2 where id1<id2
    val rddEdges = sc.textFile(config.inputPath).flatMap{line =>
      if (line.startsWith("#"))
      {
        None
      }
      else
      {
        val titleToId = broadcastMap.value
        val p = line.split(" ") 
        val page1 = p(0).substring(32, p(0).length()-1)
        val page2 = p(2).substring(32, p(2).length()-1)
        val page1Unescape = StringEscapeUtils.unescapeJava(page1)
        val page2Unescape = StringEscapeUtils.unescapeJava(page2)
        if (!(page1Unescape.forall(_.isDigit)) && !(page2Unescape.forall(_.isDigit)) && !page1.startsWith("file:") && !page2.startsWith("file:")  && !page1.contains(",") && !page2.contains(","))
        {
          val id1 = titleToId(page1Unescape)
          val id2 = titleToId(page2Unescape)
          if (id1 < id2)
            Seq(Edge(id1,id2,1.0))
          else 
            Seq(Edge(id2,id1,1.0))
        }
        else
          None
      }
    }.groupBy(x => x.srcId.toString()+":"+x.dstId.toString()).filter(_._2.size >= 2).map(_._2.head)
    
    // get vertices
    val rddVertices = pageIds.map( v => (v._2,v._1))

    // create graph from vertices and edges
    val graph = Graph(rddVertices,rddEdges,"")
    
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
          buf.append(Edge(v._1,vid,triangles.toFloat/(v._2.numTriangles.toFloat * 2.0F) * v._2.clusterCoef))
        }
      }
      buf
      }
    
    val semVertices = graph3.mapVertices((vid,vd) => (vd.name.toLowerCase(),vd.numTriangles)).vertices.filter(_._2._2 > 0);
    
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