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

package io.seldon.facebook.user;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.seldon.graph.AdjacencyGraph;
import io.seldon.graph.algorithm.BetweennessCentrality;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author philipince
 *         Date: 09/09/2013
 *         Time: 16:55
 */
public class FacebookBetweenessCentrality {
    private static final Logger logger = Logger.getLogger(FacebookBetweenessCentrality.class);
    private final Boolean logStats;
    private final AtomicInteger counter = new AtomicInteger(0);

    BiMap<FacebookUser, Integer> indexDict = HashBiMap.create();

    Map<Long, FacebookUser> uidDict = new HashMap<>();
    int currentIndex = 0;
    private AdjacencyGraph adjacencyGraph;

    public FacebookBetweenessCentrality(FacebookFriendConnectionGraph facebookFriendConnectionGraph, Boolean logStats) {
        List<FacebookUser> friends = facebookFriendConnectionGraph.friends;
        List<FacebookFriendConnection> conns = facebookFriendConnectionGraph.connections;
        this.logStats = logStats;
        this.adjacencyGraph = new AdjacencyGraph(friends.size());
        for(FacebookUser user : friends){

            uidDict.put(user.getUid(), user);
            indexDict.put(user, currentIndex++);
        }
        for(FacebookFriendConnection conn : conns){
            if(conn.uid1==null || conn.uid2==null)
                continue;
            FacebookUser uidVal1 = uidDict.get(conn.uid1);
            FacebookUser uidVal2 = uidDict.get(conn.uid2);
            if(uidVal1==null || uidVal2==null)
                continue;
            addEdge(uidVal1, uidVal2);
        }
        this.adjacencyGraph.complete();
    }

    private void addEdge(FacebookUser user1, FacebookUser user2){
        Integer user1Index = indexDict.get(user1);
        Integer user2Index = indexDict.get(user2);
        adjacencyGraph.addEdge(user1Index, user2Index);
    }

    public List<FacebookBetweennessCentralityResult> orderFriendsByCentralityDesc(ThreadPoolExecutor executor){
        if(logStats && (counter.getAndIncrement() % 10 == 0)){
            adjacencyGraph.logStats();
        }
        final double[] scores = new BetweennessCentrality(executor).calculateCentrality(adjacencyGraph);
        Integer[] indexes = new Integer[scores.length];
        for(int i = 0; i<indexes.length; i++){
            indexes[i] = i;
        }
        Map<Integer, FacebookUser> indexRev = indexDict.inverse();
        Arrays.sort(indexes, new Comparator<Integer>() {
            @Override
            public int compare(final Integer o1, final Integer o2) {
                return (-1)*Double.compare(scores[o1], scores[o2]);
            }
        });

        List<FacebookBetweennessCentralityResult> sortedUsers =
                new ArrayList<>(adjacencyGraph.noOfNodes());
        for(int index : indexes){
            FacebookUser facebookUser = indexRev.get(index);
            if (facebookUser == null) {
                logger.error(String.format("facebook user was null when trying to reattach after centrality calc." +
                        " Index was : %d, indexes size as : %d, indexRev size was : %d, score was : %.2f", index, indexes.length, indexRev.size(), scores[index]));
                logger.error("Dump of indexRev: " + indexRev.toString());
                continue;
            }
            sortedUsers.add(new FacebookBetweennessCentralityResult(facebookUser, scores[index]));
        }
        return sortedUsers;
    }



    public static class FacebookBetweennessCentralityResult extends FacebookUser{

        public final Double score;

        public FacebookBetweennessCentralityResult(FacebookUser facebookUser, double score) {
            this.uid = facebookUser.uid;
            this.uid2 = facebookUser.uid2;
            this.isAppUser = facebookUser.isAppUser;
            if(facebookUser.getIsAppUser()==null){
                logger.info(facebookUser.uid + " is null");
            }
            this.mutualFriendsCount = facebookUser.mutualFriendsCount;
            this.likesCount = facebookUser.likesCount;
            this.score = score;
        }
    }

//    public static void main(String args[]){
//        long timeBefore = System.currentTimeMillis();
//        FacebookBetweenessCentrality centrality = new FacebookBetweenessCentrality(FacebookFriendConnectionGraph.build("CAACEdEose0cBADoNKf7L0k5Wg9HOLMZA9tTV5H7lIelatgZC70AtknJpBfo48BZBaQzsnDXxq96ZAk07cwYvXZAkXXBQRwRKo2whrj3xE1MT1ZAK4YnLPhvWyPoz2aZC8IwPqplsIzO8FKxugFdhghNJmK4475U2JmJNT4MWS23pc3SSPtC7GT6ZCViOhViqpYNjd1XugtU8mQZDZD"));
//        long timeTaken = System.currentTimeMillis()- timeBefore;
//        System.out.println("Time taken for fb queries is "+timeTaken);
//        timeBefore = System.currentTimeMillis();
//        centrality.orderFriendsByCentralityDesc();
//        timeTaken = System.currentTimeMillis()- timeBefore;
//        System.out.println("Time taken for alg is " + timeTaken);
//
////
////        System.out.println(Arrays.deepToString(centrality.adjacencyGraph.adjArray));
//    }
}
