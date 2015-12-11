/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.api.controller;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.business.PluginBusinessService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class SeldonPluginController {

	@Autowired
	private ResourceServer resourceServer;
	
	@Autowired
	private PluginBusinessService pluginService;
	
	@RequestMapping(value="/plugins/telepath/{itemId}", method = RequestMethod.GET)
	public @ResponseBody Object retrieveUser(@PathVariable String itemId,
			HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysItem((ConsumerBean)con, itemId);
			try {
					String attrName = Util.getAttrName(req);
					JsonNode json = pluginService.telepath_tag_prediction((ConsumerBean)con, itemId, attrName);
					String jStr = json.toString();
					return jStr;
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
		ApiLogger.log("telepath.item_id",start,new Date(),con,res,req);
		
		return res;
	}
	
}
