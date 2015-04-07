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
import javax.servlet.http.HttpServletResponse;

import io.seldon.api.APIException;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.util.JSONPObject;
import io.seldon.api.jdo.Token;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.TokenBean;
import io.seldon.api.service.AuthorizationServer;

/**
 * @author claudio
 */

@Controller
@RequestMapping(value="/token")
public class TokenController {

	@Autowired
	private AuthorizationServer authorizationServer;

	@RequestMapping(method = RequestMethod.GET)
	public @ResponseBody Object getToken(HttpServletRequest req, HttpServletResponse resp,
			@RequestParam(value = "jsonpCallback", required = false) String callback) {
		/*Resource res = new Resource();
		APIException apiEx = new APIException(APIException.HTTPMETHOD_NOT_VALID);
		res = new ErrorBean(apiEx);
		resp.setStatus(apiEx.getHttpResponse());
		return res;*/
		ResourceBean res =  createToken(req,resp);
		if (callback != null) {
			return asCallback(callback, res);
		} else {
			return res;
		}
	}

	@RequestMapping(method = RequestMethod.POST)
	public @ResponseBody ResourceBean createToken(HttpServletRequest req, HttpServletResponse resp) {
		Date start = new Date();
		ResourceBean res = null;
		try {
			/*resp.setContentType(Constants.CONTENT_TYPE_JSON);
			resp.setHeader(Constants.CACHE_CONTROL, Constants.NO_CACHE);*/
			//retrieve token
			Token token = authorizationServer.getToken(req);
			//if successful
			resp.setStatus(HttpServletResponse.SC_OK);
			TokenBean bean = new TokenBean(token);
			MemCachePeer.put(MemCacheKeys.getTokenBeanKey(bean.getAccess_token()), bean, (int) bean.getExpires_in());
			res = bean;
		}
		catch(APIException e) {
			ApiLoggerServer.log(this, e);
			ErrorBean bean = new ErrorBean(e);
			resp.setStatus(e.getHttpResponse());
			res = bean;
		}
		catch (Exception e) {
			ApiLoggerServer.log(this, e);
			APIException apiEx = new APIException(APIException.GENERIC_ERROR);
			ErrorBean bean = new ErrorBean(apiEx);
			resp.setStatus(apiEx.getHttpResponse());
			res = bean;
			e.printStackTrace();
		}
		ApiLogger.log("token",start,new Date(),res,res,req);
		return res;
	}
	
	 private JSONPObject asCallback(String callbackName, Object valueObject) {
	        return new JSONPObject(callbackName, valueObject);
	    }
}