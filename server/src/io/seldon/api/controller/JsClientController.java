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
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.logging.CtrFullLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.RecommendationService;
import io.seldon.api.resource.service.UserService;
import io.seldon.api.resource.service.business.ActionBusinessService;
import io.seldon.api.resource.service.business.ItemBusinessService;
import io.seldon.api.resource.service.business.RecommendationBusinessService;
import io.seldon.api.resource.service.business.UserBusinessService;
import io.seldon.api.statsd.StatsdPeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.RecommendationUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.util.JSONPObject;

/**
 * Created by: marc on 05/07/2012 at 11:17
 */
@Controller
@RequestMapping("/js")
public class JsClientController {
    private static Logger logger = Logger.getLogger(JsClientController.class.getName());
    @Autowired
    private UserBusinessService userBusinessService;

    @Autowired
    private ActionBusinessService actionBusinessService;

    @Autowired
    private ItemBusinessService itemBusinessService;

    @Autowired
    private RecommendationBusinessService recommendationBusinessService;
    
    
    @Autowired
    private MessageSource messageSource;

    private ConsumerBean retrieveConsumer(HttpSession session) {
        return (ConsumerBean) session.getAttribute("consumer");
    }

    @ExceptionHandler(value = APIException.class)
    public @ResponseBody
    JSONPObject handleException(APIException ex, HttpServletRequest request) {
        String jsonpCallback = request.getParameter("jsonpCallback");
        if (StringUtils.isBlank(jsonpCallback)) {
            jsonpCallback = "jsonpCallback";
        }
        return asCallback(jsonpCallback, new ErrorBean(ex));
    }

    @RequestMapping("/user/new")
    public
    String registerUser(HttpSession session,
                             @RequestParam("id") String userId,
                             @RequestParam(value = "fb", required = false) Boolean facebookEnabled,
                             @RequestParam(value = "fbId", required = false) String facebookId,
                             @RequestParam(value = "fbToken", required = false) String facebookToken,
                             @RequestParam(value = "fbAppId", required = false) String facebookAppId,                             
                             @RequestParam("jsonpCallback") String callback,
                             @RequestParam(value = "async", defaultValue="false", required = false) Boolean async,
                             @CookieValue(value = "rlId_ctu", required = false) String inviterCookie) {
        return "forward:/acquire/user/new";
    }

    @RequestMapping("/action/new")
    public
    @ResponseBody
    JSONPObject registerAction(HttpSession session,
                             @RequestParam("user") String userId,
                             @RequestParam("item") String itemId,
                             @RequestParam("type") Integer type,
                             @RequestParam("jsonpCallback") String callback,
                             @RequestParam(value = "source", required = false) String referrer,
                             @RequestParam(value = "rectag", required = false) String recTag,                             
                             @RequestParam(value = "rlabs", required = false) String rlabs,
                             @RequestParam(value = "zehtg", required = false) String req) {
        final ConsumerBean consumerBean = retrieveConsumer(session);
        MDCKeys.addKeys(consumerBean, userId, itemId,recTag);
        //added zehtg parameter as additional option for rlabs
        if(StringUtils.isNotBlank(req)) { rlabs=req; }
        logger.debug("Creating action for consumer: " + consumerBean.getShort_name());
        ActionBean actionBean = createAction(userId, itemId, type, referrer,recTag);
        boolean isCTR = StringUtils.isNotBlank(rlabs);

        return asCallback(callback, actionBusinessService.addAction(consumerBean, actionBean, isCTR, rlabs,recTag));
    }
    


    @RequestMapping("/share")
    public
    String shareItem(HttpSession session,
                          @RequestParam("consumer_key") String consumerKey,
                          @RequestParam("user") String userId,
                          @RequestParam(value = "keywords", required = false) String keywords,
                          @RequestParam(value = "item", required = false, defaultValue = "") String itemId,
                          @RequestParam(value = "algorithms", required = false) String algorithms,
                          @RequestParam(value= "full", defaultValue="false", required=false) String full,
                          @RequestParam(value = "limit", defaultValue = "10") Integer userLimit,
                          @RequestParam(value = "shown", defaultValue = "4", required = false) Integer usersShown,
                          @RequestParam(value = "locations", required = false) String locations,
                          @RequestParam(value = "categories", required = false) String categories,
                          @RequestParam(value = "nonAppUsers", required = false) String nonAppUsers,
                          @RequestParam(value = "demographics", required = false) String demographics,
                          @RequestParam(value = "fbToken", required = false) String facebookToken,                          
                          @RequestParam(value = "impression", defaultValue="true", required=false) String impressionEnabledString,
                          @RequestParam(value = "addUser", defaultValue="false", required = false) Boolean addUser,                          
                          @RequestParam(value = "asyncUserAdd", defaultValue="false", required = false) Boolean async, 
                          @RequestParam(value = "fbId", required = false) String facebookId,
                          @RequestParam(value = "fbAppId", required = false) String facebookAppId, 
                          @RequestParam("jsonpCallback") String callback) {
        return "forward:/acquire/share";
    }
    
    @RequestMapping("/tracking")
    public
    String tracking(HttpSession session,
                          @RequestParam("consumer_key") String consumerKey,
                          @RequestParam("user") String userId,
                          @RequestParam("event") String eventType,
                          @RequestParam("jsonpCallback") String callback) {
        
        return "forward:/acquire/tracking";
    }

    @RequestMapping("/recommendations")
    public
    @ResponseBody
    JSONPObject userRecommendations(HttpSession session,
    								@RequestParam(value = "attributes", required = false) String attributes,
                                    @RequestParam(value = "item", required = false) String itemId,
                                    @RequestParam(value = "rlabs", required = false) String lastRecommendationListUuid,
                                    @RequestParam(value = "zehtg", required = false) String lastRecommendationListUuid2,
                                    @RequestParam(value = "dimension", defaultValue = "0") Integer dimensionId,
                                    @RequestParam(value = "limit", defaultValue = "10") Integer recommendationsLimit,
                                    @RequestParam(value = "algorithms", required = false) String algorithms,
                                    @RequestParam(value = "source", required = false) String referrer,                                    
                                    @RequestParam(value = "rectag", required = false) String recTag,                                    
                                    @RequestParam("user") String userId,
                                    @RequestParam("jsonpCallback") String callback) {
        final ConsumerBean consumerBean = retrieveConsumer(session);
        MDCKeys.addKeys(consumerBean, userId, itemId,recTag);
        //added zehtg parameter as additional option for rlabs
    	if(StringUtils.isNotBlank(lastRecommendationListUuid2)) { lastRecommendationListUuid=lastRecommendationListUuid2;}
        logger.info("Retrieving recommendations for user " + userId + ", consumer: " + consumerBean.getShort_name()+" with tag "+recTag);
        final ResourceBean recommendations = getRecommendations(consumerBean, userId, itemId, dimensionId, lastRecommendationListUuid, recommendationsLimit, attributes,algorithms,referrer,recTag);
        //tracking recommendations impression
        StatsdPeer.logImpression(consumerBean.getShort_name(),recTag);
        CtrFullLogger.log(false, consumerBean.getShort_name(), userId, itemId,recTag);
        return asCallback(callback, recommendations);
    }


    private ResourceBean getRecommendations(ConsumerBean consumerBean, String userId, String itemId, Integer dimensionId, String lastRecommendationListUuid, Integer recommendationsLimit, String attributes,String algorithms,String referrer,String recTag) {
        Long internalItemId = null;
        if (itemId != null) {
            try {
                internalItemId = ItemService.getInternalItemId(consumerBean, itemId);
            } catch (APIException e) {
                logger.warn("userRecommendations: item not found.");
            }
        }
        List<String> algList = null;
        if (algorithms != null && !algorithms.isEmpty()) {
            logger.debug("ALGORITHM STRING: " + algorithms);
            algList = Arrays.asList(algorithms.split(Util.algOptionSeparator));
            logger.debug("alglist size: " + algList.size());
        }
        logger.debug("JsClientController#getRecommendations: internal ID => " + internalItemId);
        logger.debug("JsClientController#getRecommendations: last recommendation list uuid => " + lastRecommendationListUuid);
        return recommendationBusinessService.recommendedItemsForUser(consumerBean, userId, internalItemId, dimensionId, lastRecommendationListUuid, recommendationsLimit, attributes,algList,referrer,recTag);
    }

    // TODO category, tags
    private ItemBean createItem(ConsumerBean c, String itemId, String title, String type, String category, String subcategory, String tags, String imgUrl) {
    	ItemBean item = null;
    	
    	try {
    		item = ItemService.getItem(c, itemId, false);
    	}
    	catch(APIException e) {
    		//item does not exist
    		return item;
    	}
    	
    	//item not imported yet
    	if(item.getType() == Constants.ITEM_NOT_VALID) {
    		return null;
    	}
    	//Item Type
        int iType = item.getType();
        if ( type != null && type.length()>0 ) {
            iType = Integer.getInteger(type);
        }
    	
        //Attributes
        final Map<String, String> attributes = item.getAttributesName();
        final Map<String, String> newAttributes = new HashMap<>();
        //title
        if(title != null && title.length()>0 && !title.equals(attributes.get(Constants.ITEM_ATTR_TITLE))) {
        	newAttributes.put(Constants.ITEM_ATTR_TITLE, title.trim());
        }
        //image
        if(imgUrl != null && imgUrl.length()>0 && !imgUrl.equals(attributes.get(Constants.ITEM_ATTR_IMG))) {
        	newAttributes.put(Constants.ITEM_ATTR_IMG, imgUrl.trim());
        }
        //category
        if(category != null && category.length()>0 && !category.equals(attributes.get(Constants.ITEM_ATTR_CAT))) {
        	newAttributes.put(Constants.ITEM_ATTR_CAT, category.trim());
        }
        //subcategory
        if(subcategory != null && subcategory.length()>0 && !subcategory.equals(attributes.get(Constants.ITEM_ATTR_SUBCAT))) {
        	newAttributes.put(Constants.ITEM_ATTR_SUBCAT, subcategory.trim());
        	
        }
        //tags
        if(tags != null && tags.length()>0 && !tags.equals(attributes.get(Constants.ITEM_ATTR_TAGS))) {
        	newAttributes.put(Constants.ITEM_ATTR_TAGS, tags.trim());
        }
        
        //Check if the item needs to be updated
        boolean update = false;
        if(iType != item.getType()) {
        	item.setType(iType);
        	update = true;
        }
        if(newAttributes.size()>0) {
        	item.setAttributesName(newAttributes);
        	update = true;
        }
        if(update) {
        	return item;
        }
        else {
        	return null; 	
        }
    }

    private ActionBean createAction(String userId, String itemId, Integer type,String referrer,String recTag) {
        int safeType = type == null ? 0 : type;
        ActionBean a = new ActionBean(null, userId, itemId, safeType, new Date(), 0.0, 1);
        a.setReferrer(referrer);
        a.setRecTag(recTag);
        return a;
    }

    

    private JSONPObject asCallback(String callbackName, Object valueObject) {
        return new JSONPObject(callbackName, valueObject);
    }


}
