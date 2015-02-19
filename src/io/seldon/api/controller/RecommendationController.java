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

package io.seldon.api.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.seldon.api.resource.service.business.RecommendationBusinessService;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;
import org.apache.commons.lang.BooleanUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import io.seldon.api.Constants;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.RecommendationsBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.ResourceBean;

@Controller
public class RecommendationController {
	private static Logger logger = Logger.getLogger(RecommendationController.class.getName());

    @Autowired
    private RecommendationBusinessService recommendationBusinessService;


    @Autowired
    private RecommendationService recommendationService;

    @RequestMapping(value="/users/{userId}/recommendations", method = RequestMethod.GET)
	public @ResponseBody
    ResourceBean retrievRecommendations(@PathVariable String userId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysUser((ConsumerBean)con, userId);
            res = recommendationBusinessService.recommendationsForUser((ConsumerBean) con, req, userId);
        }
		ApiLogger.log("users.user_id.recommendations",start,new Date(),con,res,req);
		return res;
	}

	@RequestMapping(value="/users/{userId}/recommendations/{itemId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveRecommendation(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		Integer itemType = Util.getType(req);
		Integer dimension = Util.getDimension(req);
		List<String> algorithms = Util.getAlgorithms(req);
		if(dimension == null) { dimension = Constants.DEFAULT_DIMENSION; }
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeys((ConsumerBean)con, userId, itemId);
				res = recommendationService.getRecommendation((ConsumerBean)con,userId,itemType,dimension,itemId, Constants.POSITION_NOT_DEFINED,algorithms);
			}
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.GENERIC_ERROR);
				res = new ErrorBean(apiEx);
			}
		}
		ApiLogger.log("users.user_id.recommendations.item_id",start,new Date(),con,res,req);
		return res;
	}	
	
	//Method to sort a list of item using the recommendation algorithm
	@RequestMapping(value="/users/{userId}/recommendations", method = RequestMethod.POST)
	public @ResponseBody ResourceBean sort(@RequestBody RecommendationsBean recs,@PathVariable String userId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		String usedAlgorithm = null;
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysUser((ConsumerBean)con, userId);
			if(recs != null) {
				logger.info("Calling sort method for" + recs.getSize() + " elements");
			}
			else {
				logger.info("Calling sort method with an empty list");
			}
			try {
				List<String> algorithms = Util.getAlgorithms(req);
				Object[] pair = RecommendationService.sort((ConsumerBean) con, userId, recs, algorithms);
				res = (RecommendationsBean)pair[0];
				usedAlgorithm = (String)pair[1];
			}
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.GENERIC_ERROR);
				res = new ErrorBean(apiEx);
			} 
		}
		else {
			res = con;
		}
		
		ApiLogger.log("users.user_id.recommendations",start,new Date(),con,res,req,usedAlgorithm);
		return res;
	}

	
	//Method to retrieve a list of users from the user trust graph that might like a specific item (recommended users)
	//the linktype parameter will filter for a specific type of link in the trustgraph and a specific set of actionType
	// the keyword parameter will add further keyword to use in the algorithm
	@RequestMapping(value="/users/{userId}/recommendations/{itemId}/trustgraph", method = RequestMethod.GET)
	public @ResponseBody ResourceBean getRecommendedUsers(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeys((ConsumerBean)con, userId, itemId);
				res = recommendationService.getRecommendedUsers((ConsumerBean)con,userId,itemId,Util.getLinkType(req),Util.getKeywords(req),Util.getLimit(req));
			}
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.GENERIC_ERROR);
				res = new ErrorBean(apiEx);
			}
		}
		ApiLogger.log("users.user_id.recommendations.item_id.trustgraph",start,new Date(),con,res,req);
		return res;
	}
	
	
	@RequestMapping(value="/users/{userid}/recommendedfriends", method = RequestMethod.GET)
	public 
	@ResponseBody
    JSONPObject findFriends(
        @PathVariable String userid, 
        @RequestParam(value = "keywords", required = false) String keywords,
        @RequestParam(value = "item", required = false, defaultValue = "") String itemId,
        @RequestParam(value = "algorithms", required = false) String algorithms,
        @RequestParam(value= "full", defaultValue="false", required=false) String full,
        @RequestParam(value= "limit", defaultValue="10", required=false) String lim,
        @RequestParam(value = "shown", defaultValue = "4", required = false) Integer usersShown,
        @RequestParam(value = "impression", defaultValue="true", required=false) String impressionEnabledString,
        HttpServletRequest req
	){
        final boolean impressionEnabled = BooleanUtils.toBoolean(impressionEnabledString);
        int limit = Integer.parseInt(lim);
		Date start = new Date();
        ResourceBean con = ResourceServer.validateResourceRequest(req);
        ResourceBean responseBean; 
        
        if (con instanceof ConsumerBean) {       	
        	ConsumerBean co = (ConsumerBean) con;
        	MDCKeys.addKeys(co, userid, itemId);
    	    Multimap<String,String> dict = HashMultimap.create();

            List<String> algorithmsArr = new LinkedList<>();
            if(algorithms != null && algorithms.length() > 0)
            {
                String[] parts = algorithms.split(",");
                for(String part : parts)
                    algorithmsArr.add(part.trim());
            }
            if(keywords != null && keywords.length() > 0)
            {
            	String[] parts = keywords.split(",");
            	for(String part : parts){
            		dict.put("keywords", part.trim());
            	}
            }
              
           //limit before: Constants.DEFAULT_RESULT_LIMIT
            ListBean recommendedUsers = recommendationService.getRecommendedUsers(
            		co, userid, itemId,
                    null, algorithmsArr,
                    limit, usersShown, dict, impressionEnabled
            );
            
            //logic for the boolean ...
             if(!BooleanUtils.toBoolean(full)){
                 List<String> usersList = new LinkedList<>();
                 //List<ResourceBean> beansList = new LinkedList<ResourceBean>();
                 
                 for (ResourceBean resourceBean : recommendedUsers.getList()) {
                	 
                     RecommendedUserBean recommendedUserBean =  (RecommendedUserBean) resourceBean;
                     String friendId = recommendedUserBean.getUser();
                     RecommendedUserBean recommendedFriend = new RecommendedUserBean();
                     recommendedFriend.setUser(friendId);
                     //beansList.add(recommendedFriend);
                      usersList.add(friendId);
                     //String friendId = recommendedUserBean.getUser();
                     //usersList.add(friendId);
                 }
                 return new JSONPObject("recommendedFriends", usersList) ;            
             }
             return new JSONPObject("recommendedFriends", recommendedUsers);
        } else {
            responseBean = con;
        }
        ApiLogger.log("users.user_id.recommendations.item_id",start,new Date(),con,responseBean,req);
        return new JSONPObject("recommendedFriends", responseBean);
    }
	
	
	@RequestMapping("/share")
    public
    @ResponseBody
    Object shareItem(HttpSession session,
                          @RequestParam("consumer_key") String consumerKey,
                          @RequestParam("user") String userId,
                          @RequestParam(value = "keywords", required = false) String keywords,
                          @RequestParam(value = "item", required = false, defaultValue = "") String itemId,
                          @RequestParam(value = "algorithms", required = false) String algorithms,
                          @RequestParam(value= "full", defaultValue="false", required=false) String full,
                          @RequestParam(value = "limit", defaultValue = "10") Integer userLimit,
                          @RequestParam(value = "locations", required = false) String locations,
                          @RequestParam(value = "shown", defaultValue = "4", required = false) Integer usersShown,
                          @RequestParam(value = "categories", required = false) String categories,
                          @RequestParam(value = "impression", defaultValue="true", required=false) String impressionEnabledString,
                          @RequestParam(value = "demographics", required = false) String demographics) {
      
		final ConsumerBean consumerBean = (ConsumerBean) session.getAttribute("consumer");
		MDCKeys.addKeys(consumerBean, userId, itemId);
        final boolean impressionEnabled = BooleanUtils.toBoolean(impressionEnabledString);
        
        List<String> algorithmsArr = new LinkedList<>();
        if(algorithms != null && algorithms.length() > 0)
        {
            String[] parts = algorithms.split(",");
            for(String part : parts)
                algorithmsArr.add(part.trim());
        }
        Multimap<String,String> dict = HashMultimap.create();

        if(keywords != null && keywords.length() > 0)
        {
            String[] parts = keywords.split(",");
            for(String part : parts){
                dict.put("keywords", part.trim());
            }
        }
        if(locations != null && locations.length() > 0)
        {
            String[] parts = locations.split(",");
            for(String part : parts){
                dict.put("locations", part.trim());
            }
        }
        if(categories != null && categories.length() > 0)
        {
            String[] parts = categories.split(",");
            for(String part : parts){
                dict.put("categories", part.trim());
            }
        }
        if(demographics != null && demographics.length() > 0)
        {
            String[] parts = demographics.split(",");
            for(String part : parts){
                dict.put("demographics", part.trim());
            }
        }
        ListBean recommendedUsers = recommendationBusinessService.recommendUsers(
                consumerBean, userId, itemId,
                null, algorithmsArr, userLimit==null? Constants.DEFAULT_RESULT_LIMIT : userLimit, usersShown, dict, null, impressionEnabled
        );
        if(!BooleanUtils.toBoolean(full)){
            List<ResourceBean> usersList = new ArrayList<>();
            for (ResourceBean resourceBean : recommendedUsers.getList()) {
                RecommendedUserBean recommendedUserBean = (RecommendedUserBean) resourceBean;
                String friendId = recommendedUserBean.getUser();
                usersList.add(new RecommendedUserBean(friendId));
            }
            recommendedUsers.setList(usersList);
        }
        return recommendedUsers;
    }
	

}
