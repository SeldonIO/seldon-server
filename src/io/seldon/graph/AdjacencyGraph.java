/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.graph;

import org.apache.log4j.Logger;
import org.apache.mahout.math.list.IntArrayList;

/**
 * Graph class specialised for speed when dealing with algorithms based on adjacent nodes.
 * Nodes can be any indexed object, but you must maintain your own dictionary of index -> object.
 * This class processes only the indexes. The complete method compacts the graph down into a single
 * array for speed but is optional.
 * @author philipince
 *         Date: 05/09/2013
 *         Time: 15:51
 */
public class AdjacencyGraph implements Graph {

    private static final Logger logger = Logger.getLogger(AdjacencyGraph.class);

    // for each indexed-node (n1), stores the indexes of the nodes that have an edge with n1.
    // So, the edges of n1, are (n1, adjacencies[n1].get(0)),
    // (n1, adjacencies[n1].get(1), ... , (adjacencies[n1].get(adjacencies[n1].size())
    // This will contain double the amount of entries than there are edges.
    IntArrayList[] adjacencies;
    // like above but in an array - quicker to access
    int[][] adjArray;
    // if true we are using adjArray, else we are using adjacencies
    boolean complete = false;

    int noOfEdges=0;

    public AdjacencyGraph(int numOfVertices){
        adjacencies = new IntArrayList[numOfVertices];
        adjArray = new int[numOfVertices][];

        // we know that all vertices will have at least one adjacency so it
        // makes sense to create this ahead of time.
        for(int i = 0; i< adjacencies.length; i++){
            adjacencies[i] = new IntArrayList();
        }
    }

    @Override
    public void addEdge(int n1, int n2){
        if(adjacencies[n1].contains(n2) ){
            return;
        }
        adjacencies[n1].add(n2);
        adjacencies[n2].add(n1);
        noOfEdges++;
    }

    @Override
    public void complete(){
        for(int i = 0; i< adjacencies.length; i++){
            adjArray[i] = adjacencies[i].toArray(new int[adjacencies[i].size()]);
        }
        adjacencies = null;
        complete = true;
    }

    @Override
    public int[] getSuccessors(int node){
        return complete? adjArray[node] : adjacencies[node].toArray(new int[adjacencies[node].size()]);
    }

    @Override
    public int noOfNodes() {
        return complete ? adjArray.length : adjacencies.length;
    }

    public void logStats() {
        int friendsCount = adjArray.length;
        int totalConns = 0;
        int max = 0;
        int min = -1;
        for(int[] conns : adjArray){
            totalConns+=conns.length;
            int length = conns.length;
            if(length > max) max = length;
            if(length < min || min ==-1) min = length;
        }
        double average = ((double)totalConns)/friendsCount;

        logger.info("Connection graph stats: friendCount="+friendsCount+", totalConns="+totalConns+", minConns="+min+", maxConns="+ max+",averageConns="+average+".");
}
}

