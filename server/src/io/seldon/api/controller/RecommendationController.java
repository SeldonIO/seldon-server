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

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.resource.service.business.RecommendationBusinessService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class RecommendationController {
	private static Logger logger = Logger.getLogger(RecommendationController.class.getName());

	@Autowired
	private ResourceServer resourceServer;

	@Autowired
    private RecommendationBusinessService recommendationBusinessService;


    @Autowired
    private RecommendationService recommendationService;

    @RequestMapping(value="/users/{userId}/recommendations", method = RequestMethod.GET)
	public @ResponseBody
    ResourceBean retrievRecommendations(@PathVariable String userId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysUser((ConsumerBean)con, userId);
			int limit = Util.getLimit(req);
			Set<Integer> dimensions;
			Integer dimension = Util.getDimension(req);
			if (dimension != null)
			{
				dimensions = new HashSet<Integer>();
				dimensions.add(dimension);
			}
			else
				dimensions = Util.getDimensions(req);
			if (dimensions.isEmpty())
				dimensions.add(Constants.DEFAULT_DIMENSION);
			res = recommendationBusinessService.recommendedItemsForUser((ConsumerBean) con, userId, dimensions, limit);
        }
		ApiLogger.log("users.user_id.recommendations",start,new Date(),con,res,req);
		return res;
	}

    //FIXME
	@RequestMapping(value="/users/{userId}/recommendations/{itemId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveRecommendation(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		Integer itemType = Util.getType(req);
		Integer dimension = Util.getDimension(req);
		List<String> algorithms = Util.getAlgorithms(req);
		if(dimension == null) { dimension = Constants.DEFAULT_DIMENSION; }
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeys((ConsumerBean)con, userId, itemId);
//				res = recommendationService.getRecommendation((ConsumerBean)con,userId,itemType,dimension,itemId, Constants.POSITION_NOT_DEFINED,algorithms);
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
	
	

}
