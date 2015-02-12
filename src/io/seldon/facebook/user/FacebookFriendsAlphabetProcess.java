
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
 * Processing the data retrieved by the FB api (friends).
 * @author dylanlentini
 *         Date: 1/11/2013
 *         Time: 12:15
 */
public class FacebookFriendsAlphabetProcess
{
    private static final Logger logger = Logger.getLogger(FacebookFriendsAlphabetProcess.class);

    List<FacebookUser> friendList = new ArrayList<FacebookUser>();
    

    public FacebookFriendsAlphabetProcess(FacebookFriendsAlphabetGraph fbGraph) 
    {
        friendList = fbGraph.friendList;
    }
    
    
    public List<FacebookFriendsRanking> orderFriendsByAlphabet()
    {
    	List<FacebookFriendsRanking> result = new ArrayList<FacebookFriendsRanking>();
    	
    	int i = 10000;
    	
    	for (FacebookUser friend : friendList)
    	{
    		i--;
    		result.add(new FacebookFriendsRanking(friend, i*1.0));
    	}
    	
        return result;
    }
    
}
