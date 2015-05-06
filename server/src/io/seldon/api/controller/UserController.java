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
import io.seldon.api.Util;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.api.resource.service.business.UserBusinessService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.util.JSONPObject;

/**
 * @author claudio
 */

@Controller
public class UserController {
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    @Autowired
    private UserBusinessService userBusinessService;
    
    @Autowired
	private UserService userService;

	@Autowired
	private ResourceServer resourceServer;

	@RequestMapping(value="/users", method = RequestMethod.GET)
	public @ResponseBody
	ResourceBean retrieveUsers(HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				String name = Util.getName(req);
				if(name != null && name.length() > 0) {
					res = UserService.getUsersByName((ConsumerBean) con, Util.getLimit(req), Util.getFull(req), name);
				}
				else {
					res = UserService.getUsers((ConsumerBean)con,Util.getLimit(req),Util.getFull(req));
				}
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
		ApiLogger.log("users", start, new Date(), con, res, req);
		return res;
	}
	
	@RequestMapping(value="/users/{userId}", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveUser(@PathVariable String userId,HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			try {
				res = UserService.getUser((ConsumerBean)con,userId,Util.getFull(req));
			}
			catch(APIException e) {
				ApiLoggerServer.log(this, e);
				res = new ErrorBean(e);
			}
			catch(NullPointerException e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.USER_NOT_FOUND);
				res = new ErrorBean(apiEx);
			}
			catch(Exception e) {
				ApiLoggerServer.log(this, e);
				APIException apiEx = new APIException(APIException.GENERIC_ERROR);
				res = new ErrorBean(apiEx);
			}
		}
		ApiLogger.log("users.user_id",start,new Date(),con,res,req);
		return res;
	}	
	
	@RequestMapping(value="/users", method = RequestMethod.POST)
	public @ResponseBody
    ResourceBean addUser(@RequestBody UserBean bean, HttpServletRequest req) {
        Date start = new Date();
        ResourceBean con = resourceServer.validateResourceRequest(req);
        ResourceBean responseBean;
        if (con instanceof ConsumerBean) {
            try {
                final ConsumerBean consumerBean = (ConsumerBean) con;
                userService.addUser(consumerBean, bean);
                responseBean = bean;
            } catch (APIException e) {
                ApiLoggerServer.log(this, e);
                responseBean = new ErrorBean(e);
                //Keep It?
                if (e.getError_id() == APIException.USER_DUPLICATED) {
                    updateUser(bean, req, null);

                }
            } catch (Exception e) {
                ApiLoggerServer.log(this, e);
                APIException apiEx = new APIException(APIException.INCORRECT_FIELD);
                responseBean = new ErrorBean(apiEx);
            }
        } else {
            responseBean = con;
        }
        ApiLogger.log("users",start, new Date(), con, responseBean, req);
        return responseBean;
    }

    @RequestMapping(value = "/users", method = RequestMethod.PUT)
    public @ResponseBody Object updateUser(@RequestBody UserBean user, HttpServletRequest req,
            @RequestParam(value = "jsonpCallback", required = false) String callback) {
        Date start = new Date();
        ResourceBean con = resourceServer.validateResourceRequest(req);
        ResourceBean responseBean;
        if (con instanceof ConsumerBean) {
            responseBean = userBusinessService.updateUser((ConsumerBean) con, user, null, false, false);
        } else {
            responseBean = con;
        }
        ApiLogger.log("users", start, new Date(), con, responseBean, req);
        if (callback != null) {
            return asCallback(callback, responseBean);
        } else {
            return responseBean;
        }
    }

    private JSONPObject asCallback(String callbackName, Object valueObject) {
        return new JSONPObject(callbackName, valueObject);
    }

}
