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

/**
 * Abstract representation of an graph with undirected and unweighted edges. Each node must be represented by an integer, which is the index of the node
 * in some list. Processing integers is much faster than processing arbitrary data structures and we lose no
 * generality.
 * @author philipince
 *         Date: 09/09/2013
 *         Time: 15:56
 */
public interface Graph {
    /**
     *
     * Add an edge to the graph
     * @param n1 a node in the edge pair
     * @param n2 the other node in the edge pair
     */
    void addEdge(int n1, int n2);

    /**
     * Compact the graph. Call this only once you have finished adding edges.
     */
    void complete();

    /**
     * @param node a node.
     * @return the other nodes that this node is connected to. i.e. return all n_i for which (node, n_i) is an edge.
     */
    int[] getSuccessors(int node);

    int noOfNodes();
}
