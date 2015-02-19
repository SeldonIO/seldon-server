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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import com.fasterxml.jackson.databind.util.JSONPObject;
import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.service.ResourceServer;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.facebook.FBConstants;
import io.seldon.facebook.service.FacebookAppRequestConversionService;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.InteractionBean;
import io.seldon.api.resource.InteractionsBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.business.ConversionBusinessService;
import io.seldon.api.resource.service.business.InteractionBusinessService;
import io.seldon.api.resource.service.business.MgmDailyStatsBusinessService;
import io.seldon.api.resource.service.business.RecommendationBusinessService;
import io.seldon.api.resource.service.business.TrackingBusinessService;
import io.seldon.api.resource.service.business.UserBusinessService;

@Controller
@RequestMapping("/acquire")
public class AcquireController {
    
    private static final Logger logger = LoggerFactory.getLogger(AcquireController.class);

    @Autowired
    private UserBusinessService userBusinessService;

    @Autowired
    private RecommendationBusinessService recommendationBusinessService;
    
    @Autowired
    private InteractionBusinessService interactionBusinessService;

    @Autowired
    private ConversionBusinessService conversionBusinessService;

    @Autowired
    private FacebookAppRequestConversionService facebookAppRequestConversionService;

    @Autowired
    private TrackingBusinessService trackingBusinessService;

    @Autowired
    private MgmDailyStatsBusinessService mgmDailyStatsBusinessService;
    
    private ConsumerBean retrieveConsumer(HttpSession session) {
        return (ConsumerBean) session.getAttribute("consumer");
    }
    
    private UserBean createFacebookUser(String userId, Boolean facebookEnabled, String facebookId, String facebookToken,String client,String facebookAppId) {
        UserBean userBean = new UserBean(userId, userId);
        Map<String, String> attributes = new HashMap<>();
        if (facebookEnabled == null) {
            facebookEnabled = false;
        }
        attributes.put(FBConstants.FB, facebookEnabled ? "1" : "0");
        if (facebookId != null) {
            attributes.put(FBConstants.FB_ID, facebookId);
        }
        if (facebookToken != null) {
            attributes.put(FBConstants.FB_TOKEN, facebookToken);
        }
        if (JDOFactory.isDefaultClient(client))
            attributes.put(FBConstants.FB_CLIENT, client);
        if (facebookAppId != null)
            attributes.put(FBConstants.FB_APP_ID, facebookAppId);
        
        
        
        userBean.setAttributesName(attributes);
        return userBean;
    }
    
    @RequestMapping("/user/new")
    public
    @ResponseBody
    Object registerUser(HttpSession session,
                             @RequestParam("id") String userId,
                             @RequestParam(value = "fb", required = false) Boolean facebookEnabled,
                             @RequestParam(value = "fbId", required = false) String facebookId,
                             @RequestParam(value = "fbToken", required = false) String facebookToken,
                             @RequestParam(value = "fbAppId", required = false) String facebookAppId,                             
                             @RequestParam(value = "jsonpCallback", required = false) String callback,
                             @RequestParam(value = "async", defaultValue="false", required = false) Boolean async,
                             @RequestParam(value = "from_app_request", required=false, defaultValue = "false") Boolean fromAppRequest,
                             @CookieValue(value = "rlId_ctu", required = false) String inviterCookie) {
        final ConsumerBean consumerBean = retrieveConsumer(session);
        MDCKeys.addKeysUser(consumerBean, userId);
        logger.info("Creating user for consumer: " + consumerBean.getShort_name());
        UserBean userBean = createFacebookUser(userId, facebookEnabled, facebookId, facebookToken,consumerBean.getShort_name(),facebookAppId);

        ResourceBean resultBean = userBusinessService.updateUser(consumerBean, userBean, inviterCookie,async, fromAppRequest);
        
        if (callback != null) {
            return asCallback(callback, resultBean);
        } else {
            return resultBean;
        }

    }
    
    @RequestMapping("/share")
    public
    @ResponseBody
    Object shareItem(HttpSession session,
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
                          @RequestParam(value = "app_user_filter", required = false) String appUserFilter,
                          @RequestParam(value = "demographics", required = false) String demographics,
                          @RequestParam(value = "fbToken", required = false) String facebookToken,                          
                          @RequestParam(value = "impression", defaultValue="true", required=false) String impressionEnabledString,
                          @RequestParam(value = "addUser", defaultValue="false", required = false) Boolean addUser,                          
                          @RequestParam(value = "asyncUserAdd", defaultValue="false", required = false) Boolean async, 
                          @RequestParam(value = "fbId", required = false) String facebookId,
                          @RequestParam(value = "fbAppId", required = false) String facebookAppId,
                          @RequestParam(value = "from_app_request", required=false, defaultValue = "false") Boolean fromRequest,
                          @CookieValue(value = "rlId_ctu", required = false) String inviterCookie,
                          @RequestParam(value = "jsonpCallback", required=false) String callback) {
        final ConsumerBean consumerBean = retrieveConsumer(session);
        MDCKeys.addKeys(consumerBean, userId, itemId);
        
        if (addUser != null && addUser)
        {
        	long startAddUserTime = System.currentTimeMillis();
        	logger.info("Running addUser logic");
        	UserBean userBean = createFacebookUser(userId, true, facebookId, facebookToken,consumerBean.getShort_name(),facebookAppId);
            userBusinessService.updateUser(consumerBean, userBean, inviterCookie,async, fromRequest);
            logger.info("Add user logic complete in "+(System.currentTimeMillis()-startAddUserTime));
        }
        
        final boolean impressionEnabled = BooleanUtils.toBoolean(impressionEnabledString);

        List<String> algorithmsArr = new LinkedList<>();
        if(algorithms != null && algorithms.length() > 0)
        {
            String[] parts = algorithms.split(",");
            for(String part : parts)
                algorithmsArr.add(part.trim());
        }
        Multimap<String,String> dict = HashMultimap.create();

        if(keywords != null && keywords.length() > 0)
        {
            String[] parts = keywords.split(",");
            for(String part : parts){
                dict.put("keywords", part.trim());
            }
        }
        if(locations != null && locations.length() > 0)
        {
            String[] parts = locations.split(",");
            for(String part : parts){
                dict.put("locations", part.trim());
            }
        }
        if(appUserFilter != null && !appUserFilter.isEmpty())
        {
            dict.put("app_user_filter", appUserFilter);
        }
        if(categories != null && categories.length() > 0)
        {
            String[] parts = categories.split(",");
            for(String part : parts){
                dict.put("categories", part.trim());
            }
        }
        if(demographics != null && demographics.length() > 0)
        {
            String[] parts = demographics.split(",");
            for(String part : parts){
                dict.put("demographics", part.trim());
            }
        }
        ListBean recommendedUsers = null;
        try {
            recommendedUsers = recommendationBusinessService.recommendUsers(
                    consumerBean, userId, itemId,
                    null, algorithmsArr, userLimit==null? Constants.DEFAULT_RESULT_LIMIT : userLimit, usersShown, dict, facebookToken, impressionEnabled
            );
        } catch (com.restfb.exception.FacebookException e) {
            logger.error("FacebookException caught", e);
            APIException apiException = new APIException(APIException.FACEBOOK_RESPONSE);
            ErrorBean errorBean = new ErrorBean(apiException);
            if (callback != null) {
                return asCallback(callback, errorBean);
            } else {
                return errorBean;
            }
        } catch (Exception e) {
            logger.error("Exception caught", e);
            APIException apiException = new APIException(APIException.GENERIC_ERROR);
            ErrorBean errorBean = new ErrorBean(apiException);
            if (callback != null) {
                return asCallback(callback, errorBean);
            } else {
                return errorBean;
            }
        } 
//        if(itemId==null){
//            for(RecommendedUserBean recUserBean : (List<RecommendedUserBean>)(List<?>)((ListBean)recommendedUsers).getList()){
//                List<String> reasons = recUserBean.getReasons();
//                if(reasons==null){
//                    continue;
//                }
//                List<String> translatedReasons = new ArrayList<String>( reasons.size());
//                for(String reason : reasons){
//                    translatedReasons.add(messageSource.getMessage(reason, null, LocaleContextHolder.getLocale()));
//                }
//                recUserBean.setReasonTranslations(translatedReasons);
//            }
//        }
       
        if (recommendedUsers==null) {
            recommendedUsers = new ListBean();
        }
        
        if(!BooleanUtils.toBoolean(full)){
            List<String> usersList = new LinkedList<>();
            for (ResourceBean resourceBean : recommendedUsers.getList()) {
                RecommendedUserBean recommendedUserBean = (RecommendedUserBean) resourceBean;
                String friendId = recommendedUserBean.getUser();
                usersList.add(friendId);
            }
            if (callback != null) {
                return asCallback(callback, usersList);
            } else {
                return usersList;
            }
        }
        
        if (callback != null) {
            return asCallback(callback, recommendedUsers);
        } else {
            return recommendedUsers;
        }

    }

    // this is for using oauth
    @RequestMapping("/daily")
    public
    @ResponseBody
    Object dailyStats(HttpSession session, HttpServletRequest req,
                          @RequestParam("oauth_token") String oauthToken,
                          @RequestParam(value = "jsonpCallback", required = false) String callback) {
        
        final ConsumerBean consumerBean = (ConsumerBean) ResourceServer.validateResourceRequest(req);

        ResourceBean resourceBean;
        try {
            resourceBean = mgmDailyStatsBusinessService.getDailyStats(consumerBean);
        } catch (APIException e) {
            ErrorBean errorBean = new ErrorBean(e);
            resourceBean = errorBean;
        }
        
        if (callback != null) {
            return asCallback(callback, resourceBean);
        } else {
            return resourceBean;
        }
    }

    // this is for using consumer_key auth
    @RequestMapping("/daily_internal")
    public
    @ResponseBody
    Object dailyStatsInternal(HttpSession session,
                          @RequestParam("consumer_key") String consumerKey,
                          @RequestParam(value = "jsonpCallback", required = false) String callback) {
        
        final ConsumerBean consumerBean = retrieveConsumer(session);
        ResourceBean resourceBean;
        try {
            resourceBean = mgmDailyStatsBusinessService.getDailyStats(consumerBean);
        } catch (APIException e) {
            ErrorBean errorBean = new ErrorBean(e);
            resourceBean = errorBean;
        }
        
        if (callback != null) {
            return asCallback(callback, resourceBean);
        } else {
            return resourceBean;
        }
    }


    @RequestMapping("/tracking")
    public
    @ResponseBody
    Object tracking(HttpSession session,
                          @RequestParam("consumer_key") String consumerKey,
                          @RequestParam("user") String userId,
                          @RequestParam("event") String eventType,
                          @RequestParam(value = "jsonpCallback", required = false) String callback) {
        
        final ConsumerBean consumerBean = retrieveConsumer(session);
        MDCKeys.addKeysUser(consumerBean, userId);
        ResourceBean resultBean = trackingBusinessService.trackEvent(eventType, consumerBean, userId);
        
        if (callback != null) {
            return asCallback(callback, resultBean);
        } else {
            return resultBean;
        }
    }

    private JSONPObject asCallback(String callbackName, Object valueObject) {
        return new JSONPObject(callbackName, valueObject);
    }

    @RequestMapping(value="/interaction/new", method = RequestMethod.POST)
    public @ResponseBody
    ResourceBean registerInteractions(@RequestBody InteractionsBean bean, HttpSession req) {
        if(bean.getAlgParams()==null){
        return (ResourceBean)registerInteraction(req, bean.getUserId(),bean.getFriendIds().toArray(new String[0]),
                null,null,null,null,bean.getType(), bean.getSubType(), null);
        }else {
            return (ResourceBean)registerInteraction(req, bean.getUserId(),bean.getFriendIds().toArray(new String[0]),
                    bean.getAlgParams().get("keywords"), bean.getAlgParams().get("locations"),
                    bean.getAlgParams().get("categories"),bean.getAlgParams().get("demographics"),
                    bean.getType(), bean.getSubType(), null);
        }

    }


    @RequestMapping("/interaction/new")
    public 
    @ResponseBody
    Object registerInteraction(HttpSession session, 
                                    @RequestParam(value = "user1", required = true) String user1,
                                    @RequestParam(value = "user2", required = true) String[] user2,
                                    @RequestParam(value = "keywords", required = false) String keywords,
                                    @RequestParam(value = "locations", required = false) String locations,
                                    @RequestParam(value = "categories", required = false) String categories,
                                    @RequestParam(value = "demographics", required = false) String demographics,
                                    @RequestParam(value = "type", required = true) Integer type,
                                    @RequestParam(value = "subtype") Integer subType,
                                    @RequestParam(value = "jsonpCallback", required = false) String callback) {
        final ConsumerBean consumer = retrieveConsumer(session);
        MDCKeys.addKeysUser(consumer, user1);
        logger.info("Registering interaction for user " + user1 + " with " + user2 + ", type " + type + " and subtype "+ subType);
        
        Multimap<String,String> algParams = HashMultimap.create();

        if(keywords != null && keywords.length() > 0)
        {
            String[] parts = keywords.split(",");
            for(String part : parts){
                algParams.put("keywords", part.trim());
            }
        }
        if(locations != null && locations.length() > 0)
        {
            String[] parts = locations.split(",");
            for(String part : parts){
                algParams.put("locations", part.trim());
            }
        }
        if(categories != null && categories.length() > 0)
        {
            String[] parts = categories.split(",");
            for(String part : parts){
                algParams.put("categories", part.trim());
            }
        }
        if(demographics != null && demographics.length() > 0)
        {
            String[] parts = demographics.split(",");
            for(String part : parts){
                algParams.put("demographics", part.trim());
            }
        }
        ListBean resultBean = new ListBean();
        for(String user2Single: user2){
            final InteractionBean interactionBean = new InteractionBean(user1, user2Single, type, subType, algParams.isEmpty()?null:algParams, new Date());

            ResourceBean result = interactionBusinessService.registerInteraction(consumer, interactionBean);
            resultBean.addBean(result);
        }
        if (callback != null) {
            return asCallback(callback, resultBean);
        } else {
            return resultBean;
        }

    }


}
