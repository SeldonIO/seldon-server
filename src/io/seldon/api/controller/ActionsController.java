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

import com.fasterxml.jackson.databind.util.JSONPObject;
import io.seldon.api.resource.service.business.ActionBusinessServiceImpl;

import io.seldon.api.Util;
import io.seldon.api.service.ApiLoggerServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.seldon.api.APIException;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ActionService;
import io.seldon.api.service.ResourceServer;

import org.springframework.web.bind.annotation.PathVariable;

/**
 * @author claudio
 */

@Controller
public class ActionsController {

    @Autowired
    private ActionBusinessServiceImpl actionBusinessService;

    @RequestMapping(value="/users/{userId}/actions", method = RequestMethod.GET)
	public @ResponseBody Object retrieveUserActions(@PathVariable String userId, HttpServletRequest req,
			@RequestParam(value = "jsonpCallback", required = false) String callback) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeysUser((ConsumerBean)con, userId);
				res = ActionService.getUserActions((ConsumerBean)con, userId, Util.getLimit(req), Util.getFull(req));
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
		ApiLogger.log("users.user_id.actions",start,new Date(),con,res,req);
		if (callback != null) {
			return asCallback(callback, res);
		} else {
			return res;
		}
	}
	
	@RequestMapping(value="/items/{itemId}/actions", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveItemActions(@PathVariable String itemId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeysItem((ConsumerBean)con, itemId);
				res = ActionService.getItemActions((ConsumerBean)con, itemId, Util.getLimit(req), Util.getFull(req));
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
		ApiLogger.log("items.item_id.actions",start,new Date(),con,res,req);
		return res;
	}
	
	
	@RequestMapping(value="/users/{userId}/actions/{itemId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveUserItemActions(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeys((ConsumerBean)con, userId,itemId);
				res = ActionService.getActions((ConsumerBean)con,userId,itemId,Util.getLimit(req), Util.getFull(req));
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
		ApiLogger.log("users.user_id.actions.item_id",start,new Date(),con,res,req);
		return res;
	}	
	
	@RequestMapping(value="/items/{itemId}/actions/{userId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveItemUserActions(@PathVariable String userId, @PathVariable String itemId,HttpServletRequest req) {
		return retrieveUserItemActions(userId,itemId,req);
	}
	
	@RequestMapping(value="/actions/{actionId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveAction(@PathVariable String actionId, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				long aid = Long.parseLong(actionId);
				res = ActionService.getAction((ConsumerBean)con, aid, Util.getFull(req));
			}
			catch(NumberFormatException e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.NUMBER_FORMAT_NOT_VALID);
				res = new ErrorBean(apiEx);
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
		ApiLogger.log("actions.action_id",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/actions", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveActions(HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = ActionService.getRecentActions((ConsumerBean)con,Util.getLimit(req),Util.getFull(req));
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
		ApiLogger.log("actions",start,new Date(),con,res,req);
		return res;
	}
	
	@RequestMapping(value="/actions", method = RequestMethod.POST)
	public @ResponseBody
    ResourceBean addActions(@RequestBody ActionBean action, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean responseBean;
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeys((ConsumerBean)con, action.getUser(),action.getItem());
            responseBean = actionBusinessService.addAction((ConsumerBean) con, action);
        }
		else {
			responseBean = con;
		}
		ApiLogger.log("actions",start,new Date(),con,responseBean,req);
        return responseBean;
	}

	@RequestMapping(value="/actions/types", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveActionsTypes(HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = ActionService.getActionTypes((ConsumerBean)con);
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
		ApiLogger.log("actions.types",start,new Date(),con,res,req);
		return res;
	}
	
	
	//External Actions
	@RequestMapping(value="/extactions", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveExtActions(HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = ActionService.getRecentExtActions((ConsumerBean)con,Util.getLimit(req),Util.getFull(req));
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
		ApiLogger.log("extactions",start,new Date(),con,res,req);
		return res;
	}
	
	 private JSONPObject asCallback(String callbackName, Object valueObject) {
	        return new JSONPObject(callbackName, valueObject);
	    }
}
