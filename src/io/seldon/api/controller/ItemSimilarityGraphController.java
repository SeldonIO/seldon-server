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

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemSimilarityGraphService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.ErrorBean;

import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author claudio
 */

@Controller
public class ItemSimilarityGraphController {

	@Autowired
	ItemSimilarityGraphService graphService;

	@RequestMapping(value="/items/{itemId}/similaritygraph", method = RequestMethod.GET)
	public @ResponseBody
	ResourceBean retrieveSimilarityGraph(@PathVariable String itemId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				res = graphService.getGraph((ConsumerBean) con, itemId, Util.getLimit(req));
			}
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
			}
			catch(NumberFormatException e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.NUMBER_FORMAT_NOT_VALID);
				res = new ErrorBean(apiEx);
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.GENERIC_ERROR);
				res = new ErrorBean(apiEx);
			}
		}
		ApiLogger.log("items.item_id.similaritygraph",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/items/{fromItemId}/similaritygraph/{itemId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveNode(@PathVariable String fromItemId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = graphService.getNode((ConsumerBean)con,itemId, fromItemId);
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
		ApiLogger.log("items.from_item_id.similaritygraph.item_id",start,new Date(),con,res,req);
		return res;
	}	
}
