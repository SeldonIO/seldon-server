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

import com.restfb.Facebook;
import io.seldon.api.APIException;
import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.algorithm.OnlineCloseFriendsByCentrality;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author philipince
 *         Date: 15/01/2014
 *         Time: 15:09
 */
@Service
public class FacebookFriendConnectionGraphBuilder {
    public static int noOfFriendsPerQuery = 15;

    private static final Logger logger = Logger.getLogger(FacebookFriendConnectionGraphBuilder.class);

    //private static final String GET_FRIENDS = "select uid, mutual_friend_count, is_app_user from user where mutual_friend_count > 0 and uid in (select uid2 from friend where uid1 = me() ORDER BY uid2 ";
    private static final String GET_FRIENDS = "select uid, mutual_friend_count, is_app_user from user where uid in (select uid2 from friend where uid1 = me() ORDER BY uid2 ";
    private static final String GET_FRIENDS_FRIENDS_PART_1 = "select uid1, uid2 from friend where uid1 in (";
    private static final String GET_FRIENDS_FRIENDS_PART_2 = ") and uid2 in (select uid2 from friend where uid1 = me()) and uid1 < uid2";

    private AsyncFacebookClient facebookClient;

    @Autowired
    public FacebookFriendConnectionGraphBuilder(AsyncFacebookClient client){
        facebookClient = client;
    }

    public FacebookFriendConnectionGraph build(String fbToken, FacebookCallLogger fbCallLogger)
    {

        logger.info("Retrieving all facebook friend connections with token " + fbToken);
        long before = System.currentTimeMillis();

        List<FacebookUser> listOfUsers  = getFacebookFriendsMultipleCalls(fbToken);
        logger.info("User has " + listOfUsers.size()+  " friends, attempting to find connections using multiple calls");
        // i.e. we would retrieve too many connections
        if (listOfUsers.size() > 2000) {
            logger.warn("This user is too big to do centrality - reverting to mutual friends alg");
            return new FacebookFriendConnectionGraph(listOfUsers, null);
        }
        List<FacebookFriendConnection> listOfConns = getFacebookConnectionsMultipleCalls(listOfUsers, fbToken, fbCallLogger);

        long timeTaken = System.currentTimeMillis() - before;
        logger.info("All queries for facebook user took " + timeTaken + "ms");
        return new FacebookFriendConnectionGraph(listOfUsers, listOfConns);
    }

    private List<FacebookUser> getFacebookFriendsMultipleCalls(String fbToken)
    {
        List<FacebookUser> toReturn = new ArrayList<FacebookUser>();
        List<Future<List<FacebookUser>>> futures = new ArrayList<Future<List<FacebookUser>>>();
        for(int i = 0; i< 5; i++){
            String fql = GET_FRIENDS + " LIMIT 1000 " + (i==0?"":("OFFSET "+i*1000))+" )";
            futures.add(facebookClient.executeFqlQueryAsync(fql, FacebookUser.class, fbToken));
        }

        for(Future<List<FacebookUser>> future : futures){
            try {
                toReturn.addAll(future.get());
            } catch (InterruptedException e) {
                logger.error("Problem querying facebook friends for access token '"+fbToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            } catch (ExecutionException e) {
                logger.error("Problem querying facebook friends for access token '" + fbToken + "'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            }
        }
        return toReturn;
    }

    private List<FacebookFriendConnection> getFacebookConnectionsMultipleCalls(List<FacebookUser> friends, String accessToken, FacebookCallLogger fbCallLogger)
    {
        // plan is to query a few users at time for their connections. This way, the limit shouldn't get h    
        friends = new ArrayList<FacebookUser>(friends);
        List<List<Long>> uidsToQueryList = splitFriendsIntoChunksByMutualFriendCount(friends);
        List<FacebookFriendConnection> listOfConns = doAsyncConnectionRequests(uidsToQueryList, accessToken, fbCallLogger);

        return listOfConns;

    }

    /*
    private static List<List<Long>> splitFriendsIntoChunksByMutualFriendCount(List<FacebookUser> friends) {
        List<List<Long>> uidsToQueryList = new ArrayList<List<Long>>();
        // split the uids into manageable bite sizes so that Facebook doesn't choke on them.
        int mutFriendTotalCount = 0;
        Iterator<FacebookUser> iter = friends.iterator();
        List<Long> uidsToQuery = new LinkedList<Long>();
        while (iter.hasNext()) {

            FacebookUser friend = iter.next();
            int mutualFriends = friend.getMutualFriendsCount();
            if (mutualFriends >= 1000) {
                // scenario 1, a friend has more the 1000 mutual friends with the user
                // put that person into his/her own query.
                List<Long> uidsToQueryForUserWithLargeCount = new LinkedList<Long>();
                uidsToQueryForUserWithLargeCount.add(friend.getUid());
                uidsToQueryList.add(uidsToQueryForUserWithLargeCount);
            } else if (mutFriendTotalCount + mutualFriends > 1000) {
                // scenario 2, a friend tips the the number of mutual friends in the current list
                // over 1000. Start a new list and add the current friend to it. Finalise the current list.
                uidsToQueryList.add(uidsToQuery);
                uidsToQuery = new LinkedList<Long>();
                mutFriendTotalCount = 0;
                //is this missing adding the current user??????????? -> uidsToQuery.add(friend.getUid());
            } else {
                // scenario 3, we haven't reached the 1000 mutual friend limit yet , keep using the same list
                mutFriendTotalCount += mutualFriends;
                uidsToQuery.add(friend.getUid());
            }
            iter.remove();
        }
        // add the final list if not empty
        if(!uidsToQuery.isEmpty()) uidsToQueryList.add(uidsToQuery);
        return uidsToQueryList;
    }
    */


    private static List<List<Long>> splitFriendsIntoChunksByMutualFriendCount(List<FacebookUser> friends) {
        List<List<Long>> uidsToQueryList = new ArrayList<List<Long>>();
        // split the uids into manageable bite sizes so that Facebook doesn't choke on them.
        int friendsCount = 0;
        Iterator<FacebookUser> iter = friends.iterator();
        List<Long> uidsToQuery = new LinkedList<Long>();
        while (iter.hasNext())
        {
            FacebookUser friend = iter.next();
            if (friendsCount >= noOfFriendsPerQuery)
            {
                uidsToQuery.add(friend.getUid());
                uidsToQueryList.add(uidsToQuery);
                uidsToQuery = new LinkedList<Long>();
                friendsCount = 0;
            }
            else
            {
                friendsCount++;
                uidsToQuery.add(friend.getUid());
            }
            iter.remove();
        }
        // add the final list if not empty
        if(!uidsToQuery.isEmpty()) uidsToQueryList.add(uidsToQuery);
        return uidsToQueryList;
    }




    public List<FacebookFriendConnection> doAsyncConnectionRequests(Collection<List<Long>> uidsToQueryList, String accessToken, FacebookCallLogger fbCallLogger)
    {
        List<FacebookFriendConnection> listOfConns = new ArrayList<FacebookFriendConnection>();
        List<Future<List<FacebookFriendConnection>>> futures = new ArrayList<Future<List<FacebookFriendConnection>>>();
        for (List<Long> uids : uidsToQueryList) {
            final String query = GET_FRIENDS_FRIENDS_PART_1 + StringUtils.join(uids, ',') + GET_FRIENDS_FRIENDS_PART_2;
            fbCallLogger.fbCallPerformed();
            Future<List<FacebookFriendConnection>> future = facebookClient.executeFqlQueryAsync(
                    query, FacebookFriendConnection.class, accessToken);
            futures.add(future);
        }
        for (Future<List<FacebookFriendConnection>> future : futures) {
            try {
                List<FacebookFriendConnection> c = future.get();
                listOfConns.addAll(c);
            } catch (InterruptedException e) {
                logger.error("Problem querying facebook connections for access token '"+accessToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            } catch (ExecutionException e) {
                logger.error("Problem querying facebook connections for access token '"+accessToken+"'", e);
                throw new APIException(APIException.GENERIC_ERROR);
            }
        };

        return listOfConns;

    }

    private static class FaceboookFriendNo {

        @Facebook("friend_count")
        public Integer friendCount;
    }


    public static void main (String args[]) throws ExecutionException, InterruptedException, NoSuchAlgorithmException {
//        int i = 0;
//        while(i<10){

        FacebookFriendConnectionGraphBuilder builder = new FacebookFriendConnectionGraphBuilder(new AsyncFacebookClient(100, 20, "ec2-54-216-58-47.eu-west-1.compute.amazonaws.com",10000, false));
        OnlineCloseFriendsByCentrality centrality = new OnlineCloseFriendsByCentrality(builder, 1,2,true);
        for(int i =0; i< 10; i++){
            String[] users = {"CAACEdEose0cBAH2QUZBaORObx1XBJINX2ulcxMvk6GPBcdOrLebsAbcFm4TyXHaBxO7ETgWkLXioDOPMAVF86K3ypOmMZCaBSE4huEvL2Tn6RKX8253lO2Ep80pQx4v17PpAqxtOpFW9kWSv7tIZB0kIgNr5HIzkmehSKS67CebMdPerIkCmCYsdunbArMZD"};

            for(String user : users){
                System.out.println("trying user " + user);
                try{
                    long timeBefore = System.currentTimeMillis();

                    UserBean user1 = new UserBean("user");
                    Map map  = new HashMap();
                    map.put(FBConstants.FB_TOKEN, "token");
                    user1.setAttributesName(map);
                    centrality.recommendUsers("user", user1,null,new ConsumerBean("james"),10,null,null);

                    long timeTaken = System.currentTimeMillis() - timeBefore;
//
                    System.out.println("TOOK "+timeTaken+"ms");
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
//        i++;
//        }
//        long before = System.currentTimeMillis();
//        i=0;
//        ExecutorService service = Executors.newFixedThreadPool(100);
//        while(i<1000){
//            service.submit(new Runnable() {
//                @Override
//                public void run() {
//                    FacebookFriendConnectionGraph.build("CAACWIhKBSPYBAKsjddFLjQvXZBKMUiH9rfgiEOQrR2He0peAGmGIPhaZCjW6lZBfRgE0MdnDSPZA7Lap3ZBua48s4pSThlFTNHHqSivWs4jvPZA79ZC737Xr0Tr9tWfZBHyllor8YKqrQmrDamQZA3mmezrhVbW6AJ5srCEdjL4ZCZBsCb4HgGLcAbAGYhZCxI4P74wU964ZAOnr0MQZDZD"); }
//            });
//            i++;
//        }
//        service.shutdown();
//        service.awaitTermination(100, TimeUnit.DAYS);
    }


}
