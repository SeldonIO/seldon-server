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

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserTrustNodeBean;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.service.UserTrustGraphService;

import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author claudio
 */

@Controller
public class UserTrustGraphController {

    @Autowired
    private RecommendationService recommendationService;

	@Autowired
	private UserTrustGraphService userTrustGraphService;

	@RequestMapping(value="/users/{userId}/trustgraph", method = RequestMethod.GET)
	public @ResponseBody
	ResourceBean retrieveTrustGraph(@PathVariable String userId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				//if there are keywords the result would be a list of RecommendedUserBean instead of UserTrustNodeBean
				List<String> keywords = Util.getKeywords(req);
				if(keywords == null || keywords.isEmpty()) {
					res = userTrustGraphService.getGraph((ConsumerBean)con,userId,Util.getLimit(req));
				}
				else {
					res = recommendationService.getRecommendedUsers((ConsumerBean)con,userId,null,Util.getLinkType(req),Util.getKeywords(req),Util.getLimit(req));
				}
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
		ApiLogger.log("users.user_id.trustgraph",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/users/{fromUser}/trustgraph/{userId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveNode(@PathVariable String fromUser, @PathVariable String userId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = userTrustGraphService.getNode((ConsumerBean)con,userId,fromUser);
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
		ApiLogger.log("users.from_user_id.trustgraph.user_id",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/users/{userId}/trustgraph", method = RequestMethod.POST)
	public @ResponseBody Map<String, ? extends Object> addActions(@RequestBody UserTrustNodeBean bean, @PathVariable String userId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		String response = "ok";
		bean.setCenter(userId);
		if(con instanceof ConsumerBean) {
			try {	UserTrustGraphService.addEplicitLink((ConsumerBean) con, bean); }
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
				response = ((ErrorBean)res).getError_msg();
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.INCORRECT_FIELD);
				res = new ErrorBean(apiEx);
				response = ((ErrorBean)res).getError_msg();
			} 
		}
		else {
			response = ((ErrorBean)res).getError_msg();
		}
		ApiLogger.log("users.user_id.trustgraph",start,new Date(),con,res,req);
		return Collections.singletonMap("response",response);
	}	
}
