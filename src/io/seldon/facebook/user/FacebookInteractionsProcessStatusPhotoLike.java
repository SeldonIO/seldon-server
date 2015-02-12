
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

import org.apache.log4j.Logger;
import java.util.*;

/**
 * Processing the data retrieved by the FB api (statuses, photos, their likes and the users who liked them).
 * @author dylanlentini
 *         Date: 29/10/2013
 *         Time: 10:05
 */
public class FacebookInteractionsProcessStatusPhotoLike 
{
    private static final Logger logger = Logger.getLogger(FacebookInteractionsProcessStatusPhotoLike.class);

    List<FacebookLike> userStatusLikeList = new ArrayList<FacebookLike>();
    List<FacebookLike> userPhotoLikeList  = new ArrayList<FacebookLike>();
    

    public FacebookInteractionsProcessStatusPhotoLike(FacebookInteractionsGraphStatusPhotoLike fbGraph) 
    {
        userStatusLikeList = fbGraph.userStatusLikeList;
        userPhotoLikeList = fbGraph.userPhotoLikeList;
    }
    
    
    public List<FacebookFriendsRanking> orderFriendsByInteractions()
    {
    	HashMap<FacebookUser, Double> fbIdStatusLikeCount = countLikes(userStatusLikeList);
        HashMap<FacebookUser, Double> fbIdPhotoLikeCount = countLikes(userPhotoLikeList);
        
        List<HashMap<FacebookUser, Double>> countList = new ArrayList<HashMap<FacebookUser, Double>>();
        countList.add(fbIdStatusLikeCount);
        countList.add(fbIdPhotoLikeCount);
        
        List<FacebookFriendsRanking> result = scoreCounts(countList);
        
        return result;
    }
    

    private HashMap<FacebookUser, Double> countLikes(List<FacebookLike> interactionList)
    {
    	//count statuses/photos/posts likes
        //A module for counting the number of likes per friend on statuses, photos, posts, etc
        //Input: list of FacebookLike items having at least [user_id]
        //Output: hashmap of friends having key [user_id], value [count]
    	
    	HashMap<FacebookUser, Double> fbIdCount = new HashMap<FacebookUser, Double>();
    	
	 	for (FacebookLike item : interactionList)
    	{
    		if (fbIdCount.containsKey(new FacebookUser(item.getUserId())))
    		{
    			fbIdCount.put(new FacebookUser(item.getUserId()), fbIdCount.get(new FacebookUser(item.getUserId())) + 1);
    		}
    		else
    		{
    			fbIdCount.put(new FacebookUser(item.getUserId()), 1.0);
    		}
    	}
    	
    	return fbIdCount;
    	
    }
    
    public List<FacebookFriendsRanking> scoreCounts(List<HashMap<FacebookUser, Double>> countList)
    {
    	//A module for adding counts together on different datasets of status/photos/posts like/comment counts
        //Input: list of hashmap of [facebookuser,score] items
        //Output: sorted list of FacebookInteractionsResult[facebookuser,score]

    	HashMap<FacebookUser, Double> fbUserCount = new HashMap<FacebookUser, Double>();
    	fbUserCount.putAll(countList.get(0));
    	
    	int i = 0;
    	Double val = 0.0;
    	
    	for (HashMap<FacebookUser, Double> count : countList)
    	{
    		if (i>0)
    		{
    			for(FacebookUser user : count.keySet()) 
    			{
        		    if(fbUserCount.containsKey(user)) 
        		    {
        		    	val = fbUserCount.get(user);
        		    	fbUserCount.put(user,count.get(user)+val);
        		    } 
        		    else 
        		    {
        		    	fbUserCount.put(user,count.get(user));
        		    }
        		}
        			
    		}
    		i++;
    		
    	}
    	
    	//change hashmap to list of FacebookFriendsRanking
    	List<FacebookFriendsRanking> fbIntResult = new ArrayList<FacebookFriendsRanking>();
    	for (Map.Entry<FacebookUser, Double> user : fbUserCount.entrySet()) 
    	{
    	    fbIntResult.add(new FacebookFriendsRanking(user.getKey(), user.getValue()));
    	}
    	
    	//sort list by scores
    	Collections.sort(
    		fbIntResult, new Comparator<FacebookFriendsRanking>() 
    		{
    			public int compare(FacebookFriendsRanking r1, FacebookFriendsRanking r2) 
    			{
    				return r2.score.compareTo(r1.score);
    			}
    		}
        );
    	
    	//Collections.reverse(fbIntResult);
    	
    	return fbIntResult;
    }
        
    
}
