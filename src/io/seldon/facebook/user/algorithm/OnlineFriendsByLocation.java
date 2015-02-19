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

package io.seldon.facebook.user.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import io.seldon.api.logging.FacebookCallLogger;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.client.AsyncFacebookClient;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.UserBean;
import io.seldon.facebook.FBConstants;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Multimap;
import io.seldon.facebook.user.FacebookUser;

/*
 * 
 *  Friends you live within the same city, citta'
 * 
 */

@Component
public class OnlineFriendsByLocation implements FacebookUsersAlgorithm {
	
	private static final String FRIENDS_BY_LOCATION = 
			"SELECT uid " +
			"FROM user " +
			"WHERE uid in (SELECT uid2 FROM friend WHERE uid1 = me())%s "+
			"and current_location.id in (SELECT current_location.id FROM user WHERE uid = me())";
	
	private static String FRIENDS_BY_LOCATION_PAR = 
    		"SELECT uid " +     	
    		"FROM user " +
    		"WHERE uid in (SELECT uid2 FROM friend WHERE uid1 = me())%s "+
    	    "and current_location.id in ";
					
	private static final Logger logger = Logger.getLogger(OnlineFriendsByLocation.class);
	
    private static final String FRIENDS_BY_LOCATION_REASON = "locationReason";
    
    private static final String KEYWORK_DICT = "locations"; 
        
    private static final double score = 1;
    private final AsyncFacebookClient fbClient;

    @Autowired
    public OnlineFriendsByLocation(AsyncFacebookClient fbClient){
        this.fbClient = fbClient;
    }
    
	@Override
	public List<RecommendedUserBean> recommendUsers(String userId, UserBean user, String serviceName, ConsumerBean client, int resultLimit, Multimap<String, String> dict, SocialRecommendationStrategy.StrategyAim aim) {
		logger.info("Retrieving friends with close location: "+userId + ", client: "+ client.getShort_name());
		
		List<RecommendedUserBean> results = new ArrayList<>();
		
		long before = System.currentTimeMillis();

		if(user==null){
	            logger.error("Couldn't find user with id "+ userId + " when recommending friends by location.");
	            return results;
	    }
		String fbToken = user.getAttributesName().get(FBConstants.FB_TOKEN);
	    if(fbToken==null){
	    	logger.error("User with id "+ userId + " does not have an fb token - return empty results set when recommending friends by location.");
	        return results;
	    }


        FacebookCallLogger fbCallLogger = new FacebookCallLogger();
	    List<FacebookUser> userList = queryFacebook(fbToken, dict, fbCallLogger);
        fbCallLogger.log(this.getClass().getName(), client.getShort_name(), userId, fbToken);

        for(FacebookUser fbUser : userList){
            Long id = fbUser.getUid();
            results.add(new RecommendedUserBean(String.valueOf(id), null, score, Arrays.asList(FRIENDS_BY_LOCATION_REASON)));
        }
                   
        long timeTaken = System.currentTimeMillis() - before;
        logger.info("Retrieving friends by location  " + timeTaken + "ms for id "+userId);
        return results;
	        
	    }

    @Override
    public boolean shouldCacheResults() {
        return true;
    }


    private List<FacebookUser> queryFacebook(String fbToken, Multimap<String,String> dict, FacebookCallLogger fbCallLogger){

			List<FacebookUser> userList = null;
			if ( dict == null ){
                logger.info("Retrieving friends who have the same location as user.");
	            userList = fbClient.executeFqlQuery(String.format(FRIENDS_BY_LOCATION, FacebookAppUserFilterType.fromQueryParams(dict).toQuerySegment()), FacebookUser.class, fbToken);
                fbCallLogger.fbCallPerformed();
				return userList;

			}
							
			Collection<String> entriesDict = dict.get(KEYWORK_DICT);							

	        if(entriesDict.isEmpty()){        	
	            userList = fbClient.executeFqlQuery(String.format(FRIENDS_BY_LOCATION,FacebookAppUserFilterType.fromQueryParams(dict).toQuerySegment()), FacebookUser.class, fbToken);
                fbCallLogger.fbCallPerformed();
	                     
	        }else{
                logger.info("Retrieving friends who have location in "+entriesDict);
	        	String resultQuery = assembleQueryPar(entriesDict);
                userList = fbClient.executeFqlQuery(String.format(resultQuery,FacebookAppUserFilterType.fromQueryParams(dict).toQuerySegment()), FacebookUser.class, fbToken);
                fbCallLogger.fbCallPerformed();

	        }
            return userList;
		}
		
		
		private String assembleQueryPar(Collection<String> entriesDict){
			
			Iterator iter = entriesDict.iterator();
			StringBuilder sb = new StringBuilder();
			String startPar ="(";
			//String commHig ="'";
			String commLow =",";
			sb.append(FRIENDS_BY_LOCATION_PAR);
			sb.append(startPar);
			
			while(iter.hasNext()) {				 
				//sb.append(commHig);
				sb.append(iter.next());
				//sb.append(commHig);
				sb.append(commLow);
			}
			 
			sb.deleteCharAt(sb.length()-1);					
			sb.append(")");
			String result = sb.toString();
			return result;
		}


}


