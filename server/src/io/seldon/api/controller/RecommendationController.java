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
import io.seldon.api.logging.CtrFullLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.resource.service.business.RecommendationBusinessService;
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
    private ItemService itemService;

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
			
			List<String> sort = Util.getSortItems(req);
			Set<Long> sortItems = null;
			if (sort != null && sort.size() > 0)
			{
				sortItems = new HashSet<Long>();
				ConsumerBean c = (ConsumerBean) con;
				for(String item : sort)
				{
					try {
						Long internalSortId = itemService.getInternalItemId(c, item);
		        			sortItems.add(internalSortId);
		        		} catch (APIException e) {
		        			logger.warn("userRecommendations: sort item not found."+item);
		        		}
				}
			}
			
			res = recommendationBusinessService.recommendedItemsForUser((ConsumerBean) con, userId, dimensions, limit,sortItems);
			CtrFullLogger.log(false, ((ConsumerBean)con).getShort_name(), userId, null,null);
        }
		ApiLogger.log("users.user_id.recommendations",start,new Date(),con,res,req);
		return res;
	}


}
