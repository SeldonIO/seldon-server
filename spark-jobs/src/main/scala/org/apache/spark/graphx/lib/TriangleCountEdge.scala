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
package org.apache.spark.graphx.lib

import org.apache.log4j.Logger

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.GraphLoader
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.Iterator


 object TriangleCountEdge
{
   
   def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED],minTriangles : Int): Graph[(Map[Long,Int],Int), ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(VertexId, (Map[Long,Int],Int))] = {
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      } else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      if (counter >= minTriangles && et.srcId != et.dstId){
      Iterator((et.srcId, (Map(et.dstId.toLong->counter),counter)),(et.dstId,(Map(et.srcId.toLong->counter),counter)))}
      else{
        Iterator.empty
      }
    }
    // compute the intersection along edges
    val counters: VertexRDD[(Map[Long,Int],Int)] = setGraph.mapReduceTriplets(edgeFunc,(count1 : (Map[Long,Int],Int) ,count2 : (Map[Long,Int],Int)) => {
      (count1._1 ++ count2._1,count1._2+count2._2)
    })
   
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[(Map[Long,Int],Int)]) =>
        val rval = optCounter.getOrElse((Map[Long,Int](),0))
        (rval._1,rval._2/2)
    }
    
  } 
   
}