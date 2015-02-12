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

package io.seldon.graph.algorithm;

import io.seldon.api.APIException;
import io.seldon.graph.Graph;
import org.apache.log4j.Logger;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;

import java.util.*;
import java.util.concurrent.*;


/**
 * Concurrent implementation of "Ulrik Brandes: A Faster Algorithm for Betweenness Centrality. Journal of Mathematical Sociology 25(2):163-177, 2001."
 * This is available in a few libraries but the implementations are focussed on re-usability and hence are slow. For example, this is an order of
 * magnitude faster than the JUNG version.
 * @author philipince
 *         Date: 05/09/2013
 *         Time: 17:39
 */
public class BetweennessCentrality {

    private static final int MINUS_ONE = -1;
    private static final int ZERO = 0;
    private final ThreadPoolExecutor executor;
    private final Logger logger = Logger.getLogger(BetweennessCentrality.class);

    public BetweennessCentrality(ThreadPoolExecutor executor){
        this.executor = executor;
    }

    public double[] calculateCentrality(final Graph g)  {
        // normal users will return in a few hundred millis which is tiny compared to the facebook call.
        // however, the time taken expands exponentially with then number of friends a user has.
        // so, for large users (>500 friends) we split the calculation into pieces and submit them to seperate threads.
        // in a sense we are calculating shortest paths between users u_i and u_j. So we split the u_i up as below.
        // it could be split up better if we had a better idea of which u_i will require heavy processing, but it
        // is quick enough for our purposes as is.
        final int noOfNodes = g.noOfNodes();
        logger.info("centrality calc queue size is " + executor.getQueue().size());
        if(noOfNodes>500 && executor.getQueue().remainingCapacity()>100 ){

            int capacity = Runtime.getRuntime().availableProcessors();
            List<Future<double[]>> futures = new ArrayList<Future<double[]>>(capacity);
            for(int i = 0; i< capacity; i++){
                final int j = i;
                boolean isFinal = j== capacity -1;
                final int startNode = i*(noOfNodes/ capacity);
                final int endNode = isFinal? noOfNodes : (i+1) * (noOfNodes/ capacity);

                futures.add(executor.submit(new Callable<double[]>() {
                    @Override
                    public double[] call() throws Exception {
                        return calcPartial(g, noOfNodes, startNode, endNode);
                    }
                }));
            }
            double[] cumulativeScores = new double[noOfNodes];
            for(Future<double[]> future : futures){
                double[] partialScores = new double[0];
                try {
                    partialScores = future.get();
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                    throw new APIException(APIException.GENERIC_ERROR);
                }
                cumulativeScores = combinePartialScores(cumulativeScores, partialScores);
            }
            return cumulativeScores;
        }

        return calcPartial(g, noOfNodes, 0, noOfNodes);


    }

    private double[] combinePartialScores(double[] cumulativeScores, double[] partialScores) {
        for(int i = 0; i< cumulativeScores.length; i++){
            cumulativeScores[i]+=partialScores[i];
        }
        return cumulativeScores;
    }

    /**
     * the actual calculation of centrality scores. Check the paper for more details.
     * @param g
     * @param noOfNodes
     * @param startVertex
     * @param endVertex
     * @return
     */
    private double[] calcPartial(Graph g, int noOfNodes, int startVertex, int endVertex) {
        final int[] distances = new int[noOfNodes];
        final int[] shortestPathCounts = new int[noOfNodes];
        final double[] dependencies = new double[noOfNodes];
        IntArrayList[] predecessors = new IntArrayList[noOfNodes];
        double[] scores = new double[noOfNodes];


        for(int i=startVertex; i<endVertex; i++){

            Arrays.fill(distances, MINUS_ONE);
            Arrays.fill(shortestPathCounts, ZERO);
            Arrays.fill(dependencies, 0.0);
            Arrays.fill(predecessors, null);
            shortestPathCounts[i]=1;
            distances[i]=0;
            Deque<Integer> stack = new ArrayDeque<Integer>();
            Deque<Integer> queue = new ArrayDeque<Integer>();
            queue.add(i);
            while (!queue.isEmpty()) {
                final int v = queue.remove();
                stack.push(v);
                for(int w : g.getSuccessors(v)) {
                    if (distances[w] < 0) {
                        queue.add(w);
                        distances[w] = (distances[v] + 1);
                    }

                    if (distances[w] == (distances[v] + 1)) {
                        shortestPathCounts[w] += shortestPathCounts[v];
                        IntArrayList preds = predecessors[w];
                        if(preds==null) predecessors[w] = preds = new IntArrayList();

                        preds.add(v);
                    }
                }
            }

            while (!stack.isEmpty()) {
                final int w = stack.pop();

                IntArrayList wPreds = predecessors[w];

                if(wPreds!=null){
                    wPreds.forEach(new IntProcedure() {
                        @Override
                        public boolean apply(int v) {
                            dependencies[v] += ((double)shortestPathCounts[v] *  (1.0 + dependencies[w]))
                                    / shortestPathCounts[w];
                            return true;
                        }
                    });

                }
                if (w != i) {
                    scores[w] += dependencies[w];
                }
            }
        }
        return scores;

    }


}