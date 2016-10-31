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

import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.seldon.api.logging.ApiLogger;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.business.PredictionBusinessService;
import io.seldon.api.resource.service.business.PredictionBusinessServiceImpl;
import io.seldon.api.service.ResourceServer;
import io.seldon.prediction.PredictionServiceResult;

@Controller
public class PredictionController {

	@Autowired
	private PredictionBusinessService predictionBusinessService;

	@Autowired
	private ResourceServer resourceServer;
	
	@RequestMapping(value="/events", method = RequestMethod.POST)
	public @ResponseBody
    ResourceBean addEvents(@RequestBody String event, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean responseBean;
		if(con instanceof ConsumerBean) 
		{
            responseBean = predictionBusinessService.addEvent((ConsumerBean)con, event);
        }
		else {
			responseBean = con;
		}
		ApiLogger.log("events",start,new Date(),con,responseBean,req);
        return responseBean;
	}
	
	@RequestMapping(value="/predict", method = RequestMethod.POST)
	public @ResponseBody
    PredictionServiceResult prediction(@RequestBody String json, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = resourceServer.validateResourceRequest(req);
		ResourceBean responseBean;
		if(con instanceof ConsumerBean) 
		{
			String puid = req.getParameter(PredictionBusinessServiceImpl.PUID_KEY);
            return predictionBusinessService.predict((ConsumerBean)con, puid,json);
        }
		else {
			/*
			responseBean = con;
			ObjectMapper mapper = new ObjectMapper();
			JsonNode response = mapper.valueToTree(responseBean);
			return response;
			*/
			return new PredictionServiceResult();
		}
		//FIXME
		//ApiLogger.log("events",start,new Date(),con,responseBean,req);

	}
}
