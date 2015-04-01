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

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.DynamicParameterBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.DynamicParameterServer;
import io.seldon.api.service.ResourceServer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class AdminController {

    @RequestMapping(value = "/admin/dynamicparameters", method = RequestMethod.GET)
    public
    @ResponseBody
    ResourceBean retrieveDynamicParameters(HttpServletRequest req) {
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
        if (con instanceof ConsumerBean) {
        	try {
           		ConsumerBean consumerBean = (ConsumerBean) con;
            	res = DynamicParameterServer.getParametersBean(consumerBean.getShort_name());
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
		return res;
    }
    
    @RequestMapping(value="/admin/dynamicparameters", method = RequestMethod.POST)
	public @ResponseBody
    ResourceBean addDynamicParameter(@RequestBody DynamicParameterBean bean, HttpServletRequest req) {
        ResourceBean con = ResourceServer.validateResourceRequest(req);
        ResourceBean responseBean = con;
        if (con instanceof ConsumerBean) {
            try {
                final ConsumerBean consumerBean = (ConsumerBean) con;
                DynamicParameterServer.setParameterBean(consumerBean.getShort_name(), bean);
                responseBean = bean;
            } catch (APIException e) {
                ApiLoggerServer.log(this, e);
                responseBean = new ErrorBean(e);
            } catch (Exception e) {
                ApiLoggerServer.log(this, e);
                APIException apiEx = new APIException(APIException.GENERIC_ERROR);
                responseBean = new ErrorBean(apiEx);
            }
        }
        return responseBean;
    }

}
