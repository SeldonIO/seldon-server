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
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.business.ItemBusinessService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;

import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

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
public class ItemController {

    @Autowired
    private ItemBusinessService itemBusinessService;

	@Autowired
	private ItemService itemService;

    @RequestMapping(value="/items", method = RequestMethod.GET)
	public @ResponseBody Object retrieveItems(HttpServletRequest req,
			@RequestParam(value = "jsonpCallback", required = false) String callback) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		//request authorized
		if(con instanceof ConsumerBean) {
			try {
				MDCKeys.addKeysConsumer((ConsumerBean)con);
				List<String> keywords = Util.getKeywords(req);
				String name = Util.getName(req);
				Integer itemType = Util.getType(req);
				Integer dimension = Util.getDimension(req);
				if(itemType !=null && dimension == null) {
					dimension = ItemService.getDimensionbyItemType((ConsumerBean) con, itemType);
				}
				if(dimension == null) { dimension = Constants.DEFAULT_DIMENSION; }
				if(name != null && name.length() > 0) {
					res = itemService.getItemsByName((ConsumerBean)con,Util.getLimit(req),Util.getFull(req),Util.getName(req),dimension);
				}
				//all
				else {
					res = itemService.getItems((ConsumerBean)con,Util.getLimit(req),Util.getFull(req),Util.getSort(req),dimension);
				}
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
		ApiLogger.log("items",start,new Date(),con,res,req);
		if (callback != null) {
			return asCallback(callback, res);
		} else {
			return res;
		}
	}
	
	@RequestMapping(value="/items/{itemId}", method = RequestMethod.GET)
	public @ResponseBody Object retrieveUser(@PathVariable String itemId,HttpServletRequest req,
			@RequestParam(value = "jsonpCallback", required = false) String callback) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysItem((ConsumerBean)con, itemId);
			try {
				res = ItemService.getItem((ConsumerBean)con,itemId,Util.getFull(req));
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
		ApiLogger.log("items.item_id",start,new Date(),con,res,req);
		if (callback != null) {
			return asCallback(callback, res);
		} else {
			return res;
		}
	}
	
	@RequestMapping(value="/items", method = RequestMethod.POST)
	public @ResponseBody ResourceBean addItem(@RequestBody ItemBean item, HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
        ResourceBean responseBean;
        if (con instanceof ConsumerBean) {
            try {
            	if (item != null)
            		MDCKeys.addKeysItem((ConsumerBean)con, item.getId());
                itemService.addItem((ConsumerBean) con, item);
                responseBean = item;
            } catch (APIException e) {
                ApiLoggerServer.log(this, e);
                responseBean = new ErrorBean(e);
            } catch (Exception e) {
                ApiLoggerServer.log(this, e);
                APIException apiEx = new APIException(APIException.INCORRECT_FIELD);
                responseBean = new ErrorBean(apiEx);
            }
        } else {
            responseBean = con;
        }
        ApiLogger.log("items",start, new Date(), con, responseBean, req);
        return responseBean;
	}

    @RequestMapping(value = "/items", method = RequestMethod.PUT)
    public @ResponseBody ResourceBean updateItem(@RequestBody ItemBean item, HttpServletRequest req) {
        Date start = new Date();
        ResourceBean con = ResourceServer.validateResourceRequest(req);
        ResourceBean responseBean;
        if (con instanceof ConsumerBean) {
        	if (item != null)
        		MDCKeys.addKeysItem((ConsumerBean)con, item.getId());
            responseBean = itemBusinessService.updateItem((ConsumerBean) con, item);
        } else {
            responseBean = con;
        }
        ApiLogger.log("items",start, new Date(), con, responseBean, req);
        return responseBean;
    }

    @RequestMapping(value="/items/types", method = RequestMethod.GET)
	public @ResponseBody ResourceBean retrieveItemTypes(HttpServletRequest req) {
		Date start = new Date();
		ResourceBean con = ResourceServer.validateResourceRequest(req);
		ResourceBean res = con;
		if(con instanceof ConsumerBean) {
			MDCKeys.addKeysConsumer((ConsumerBean)con);
			try {
				res = ItemService.getItemTypes((ConsumerBean)con);
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
		ApiLogger.log("items.types",start,new Date(),con,res,req);
		return res;
	}
    
    private JSONPObject asCallback(String callbackName, Object valueObject) {
        return new JSONPObject(callbackName, valueObject);
    }
}
