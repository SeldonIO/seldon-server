
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
 * Processing the data retrieved by the FB api (pages and the users who liked them).
 * @author dylanlentini
 *         Date: 22/11/2013
 *         Time: 10:05
 */
public class FacebookFriendsKeywordsProcess 
{
    private static final Logger logger = Logger.getLogger(FacebookFriendsKeywordsProcess.class);

    List<FacebookFriendPageFan> friendPageList = new ArrayList<FacebookFriendPageFan>();

    
    public FacebookFriendsKeywordsProcess(FacebookFriendsKeywordsGraph fbGraph) 
    {
    	friendPageList = fbGraph.friendsPagesFans;
    }
    
    
    public List<FacebookFriendsRanking> orderFriendsByKeywords()
    {
    	HashMap<FacebookUser, Double> fbIdPagesCount = countPagesForEveryUser(friendPageList);
        
        List<HashMap<FacebookUser, Double>> countList = new ArrayList<HashMap<FacebookUser, Double>>();
        countList.add(fbIdPagesCount);
        
        List<FacebookFriendsRanking> result = scoreCounts(countList);
        
        return result;
    }
    

    private HashMap<FacebookUser, Double> countPagesForEveryUser(List<FacebookFriendPageFan> friendPageList)
    {
        HashMap<FacebookUser, Double> fbIdCount = new HashMap<FacebookUser, Double>();
    	
	 	for (FacebookFriendPageFan item : friendPageList)
    	{
    		if (fbIdCount.containsKey(new FacebookUser(item.getUid())))
    		{
    			fbIdCount.put(new FacebookUser(item.getUid()), fbIdCount.get(new FacebookUser(item.getUid())) + 1);
    		}
    		else
    		{
    			fbIdCount.put(new FacebookUser(item.getUid()), 1.0);
    		}
    	}
    	
    	return fbIdCount;
    	
    }
    
    public List<FacebookFriendsRanking> scoreCounts(List<HashMap<FacebookUser, Double>> countList)
    {
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
    	
    	return fbIntResult;
    }
        
    
}
