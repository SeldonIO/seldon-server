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

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.InteractionBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.business.InteractionBusinessService;
import io.seldon.api.resource.service.business.RecommendationBusinessService;
import io.seldon.api.resource.service.business.UserBusinessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.facebook.service.FacebookService;
import org.apache.log4j.Logger;

import io.seldon.api.service.ResourceServer;

@Controller
public class InteractionController {
	
	private static Logger logger = Logger.getLogger(RecommendationController.class.getName());
	
	@Autowired
	private RecommendationBusinessService recommendationBusinessService;
	
	@Autowired
    private FacebookService facebookService;
	
	@Autowired
    private UserBusinessService userBusinessService;
	
	@Autowired
    private InteractionBusinessService interactionBusinessService;
		
	/* 
	 * HTTP POST https://api.rummblelabs.com/users/USERID/interaction/FRIEND_USERID?o
	 * auth_token=TOKEN&type=1&subtype=1 or 2
	 */
	
	@RequestMapping(value="/users/{userid}/interaction/{friendid}", method = RequestMethod.POST)
	public @ResponseBody
	ResourceBean inviteFriends(
	       @PathVariable String userid,
	       @PathVariable String friendid,
	       @RequestParam(value= "subtype", defaultValue="1", required=false) Integer subtype,
	       @RequestParam(value = "type", required = false) Integer type,
	       HttpServletRequest req){
		//type has to be fixed
		Date start = new Date();
        ResourceBean con = ResourceServer.validateResourceRequest(req);
        ResourceBean responseBean;
        if (con instanceof ConsumerBean) {
        	ConsumerBean co = (ConsumerBean) con;
        	MDCKeys.addKeysUser(co, userid);
            logger.info("Registering interaction for user " + userid + " with " + friendid + ", type " + type + " and subtype "+ subtype);
            final InteractionBean interactionBean = new InteractionBean(userid, friendid, type, subtype, new Date());
            ApiLogger.log("users.userid.interaction.friendid",start, new Date(), con, interactionBean, req);
            return interactionBusinessService.registerInteraction((ConsumerBean) con, interactionBean); 
	    }else{
		    responseBean = con;
		}
        ApiLogger.log("users.userid.interaction.friendid",start, new Date(), con, responseBean, req);
		return responseBean;
     }
}

	