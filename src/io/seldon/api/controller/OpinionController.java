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
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.service.OpinionService;

import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author claudio
 */

@Controller
public class OpinionController {

	@RequestMapping(value="/users/{userId}/opinions", method = RequestMethod.GET)
	public @ResponseBody
	ResourceBean retrieveUserOpinions(@PathVariable String userId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				res = OpinionService.getUserOpinions((ConsumerBean)con,userId, Util.getLimit(req),Util.getFull(req));
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
		ApiLogger.log("users.user_id.opinions",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/items/{itemId}/opinions", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveItemOpinions(@PathVariable String itemId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				res = OpinionService.getItemOpinions((ConsumerBean)con,itemId,Util.getLimit(req),Util.getFull(req));
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
		ApiLogger.log("items.item_id.opinions",start,new Date(),con,res,req);
		return res;
	}
	
	
	@RequestMapping(value="/users/{userId}/opinions/{itemId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveUserOpinion(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = OpinionService.getOpinion((ConsumerBean)con,userId, itemId);
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
		ApiLogger.log("users.user_id.opinions.item_id",start,new Date(),con,res,req);
		return res;
	}	
	
	@RequestMapping(value="/items/{itemId}/opinions/{userId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveItemOpinion(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		return retrieveUserOpinion(userId,itemId,req);
	}
}
