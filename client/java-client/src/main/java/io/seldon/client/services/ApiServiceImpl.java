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
package io.seldon.client.services;

import io.seldon.client.algorithm.AlgorithmOptions;
import io.seldon.client.beans.*;
import io.seldon.client.constants.Constants;
import io.seldon.client.exception.ApiTokenException;
import io.seldon.client.exception.NoApiStateException;

import java.net.SocketTimeoutException;
import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.http.client.CommonsClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

public class ApiServiceImpl implements ApiService {

    public static final int DEFAULT_READ_TIMEOUT = 1500;
    private int readTimeout = DEFAULT_READ_TIMEOUT;

    @Autowired
    private ApiState apiState;

    private static Logger logger = Logger.getLogger(ApiServiceImpl.class.getName());
    private String token = null;
    private long tokenExpiry;
    private TokenBean tokenBean;

    public void setApiState(ApiState apiState) {
        this.apiState = apiState;
        if ( apiState.getReadTimeout() != null ) {
            readTimeout = apiState.getReadTimeout();
        }
    }

    public synchronized ResourceBean getToken() {
        Calendar calendar = Calendar.getInstance();
        long timeInMillis = calendar.getTimeInMillis();

        logger.debug("  Time in millis: " + timeInMillis);
        logger.debug("Token expires at: " + tokenExpiry);

        if ( timeInMillis < tokenExpiry ) {
            logger.debug("(Another thread has already refreshed the token)");
            return tokenBean;
        }

            if (apiState == null) {
                throw new NoApiStateException("Please set your API key and secret before using the API.");
            }

            StringBuilder stringBuilder = new StringBuilder();
            logger.info("* Fetching token...");
            stringBuilder.append(getApiRoot(true)); // attempt to use a secure connection (or fall back on apiUrl)
            stringBuilder.append("/token?");
            stringBuilder.append("consumer_key=");
            stringBuilder.append(apiState.getConsumerKey());
            stringBuilder.append("&");
            stringBuilder.append("consumer_secret=");
            stringBuilder.append(apiState.getConsumerSecret());

            ResourceBean res;
            try {
                res = getTokenResource(stringBuilder.toString());
            } catch (HttpClientErrorException e) {
                throw new ApiTokenException("Could not retrieve token -- please check your key and secret.");
            }
            if (res instanceof TokenBean) {
                TokenBean tokenBean = (TokenBean) res;
                this.tokenBean = tokenBean;
                timeInMillis = calendar.getTimeInMillis();
                this.tokenExpiry = timeInMillis + tokenBean.getExpires_in();
                this.token = tokenBean.getAccess_token();
            }

            return res;
    }

    private String getApiRoot(boolean withSecureConnection) {
        String apiRoot;
        if (withSecureConnection) {
            logger.info("** Trying secure connection...");
            apiRoot = apiState.getSecureApiUrl();
            if (apiRoot != null && apiRoot.length() > 0) {
                logger.info("*** OK");
                return apiRoot;
            }
            logger.info("*** NOK");
        }
        apiRoot = apiState.getApiUrl();
        return apiRoot;
    }

    @Override
    public ResourceBean getItems(int limit, boolean full, String keyword, String name) {
        String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, keyword, name);
        return getResource(url, ItemsBean.class);
    }

    @Override
	public ResourceBean getItems(int limit, Integer itemType, boolean full,
			String sorting) {
    	String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, null) + "&sort=" + sorting +"&type=" + itemType;
        return getResource(url, ItemsBean.class);
	}
    
    @Override
    public ResourceBean getItems(int limit, Integer itemType, boolean full, String keyword, String name) {
        String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, keyword, name) + "&type=" + itemType;
        return getResource(url, ItemsBean.class);
    }


    @SuppressWarnings({"NullableProblems"})
    public ResourceBean getItems(int limit, boolean full) {
        String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, null);
        return getResource(url, ItemsBean.class);
    }

    @SuppressWarnings({"NullableProblems"})
    @Override
    public ResourceBean getItems(int limit, int type, boolean full) {
        String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, null) + "&type=" + type;
        return getResource(url, ItemsBean.class);
    }

    @SuppressWarnings({"NullableProblems"})
	@Override
	public ResourceBean getItems(int limit, boolean full, String sorting) {
        String res = Constants.ITEMS;
        String url = getUrl(res, limit, full, null) + "&sort=" + sorting;
        return getResource(url, ItemsBean.class);
	}
	
    public ResourceBean getItem(String id, boolean full) {
        String res = Constants.ITEMS + "/" + id;
        String url = getUrl(res, full);
        return getResource(url, ItemBean.class);
    }

    public ResourceBean getUsers(int limit, boolean full) {
        return getUsers(limit, full, false);
    }

    @SuppressWarnings({"NullableProblems"})
    @Override
    public ResourceBean getUsers(int limit, boolean full, boolean withSecureConnection) {
        String res = Constants.USERS;
        String apiRoot = getApiRoot(withSecureConnection);
        String url = getUrl(apiRoot, res, limit, full, null);
        return getResource(url, UsersBean.class);
    }


    public ResourceBean getUser(String id, boolean full) {
        return getUser(id, full, false);
    }

    @Override
    public ResourceBean getUser(String id, boolean full, boolean withSecureConnection) {
        String res = Constants.USERS + "/" + id;
        String apiRoot = getApiRoot(withSecureConnection);
        String url = getUrl(apiRoot, res, full);
        return getResource(url, UserBean.class);
    }


    public ResourceBean getSimilarItems(String id, int limit) {
        String res = Constants.ITEMS + "/" + id + "/" + Constants.SIMILARITYGRAPH;
        String url = getUrl(res);
        return getResource(url, ItemGraphBean.class);
    }

    @SuppressWarnings({"NullableProblems"})
    public ResourceBean getTrustedUsers(String id, int limit) {
        String res = Constants.USERS + "/" + id + "/" + Constants.TRUSTGRAPH;
        String url = getUrl(res, limit, false, null);
        return getResource(url, UserGraphBean.class);
    }

    public ResourceBean getPrediction(String uid, String oid) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.OPINIONS + "/" + oid;
        String url = getUrl(res);
        return getResource(url, OpinionBean.class);
    }

    public ResourceBean getOpinions(String uid, int limit) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.OPINIONS;
        String url = getUrl(res, limit, true, "");
        return getResource(url, OpinionsBean.class);
    }

    public ResourceBean getItemOpinions(String itemId, int limit) {
        String res = Constants.ITEMS + "/" + itemId + "/" + Constants.OPINIONS;
        String url = getUrl(res, limit, true, "");
        return getResource(url, OpinionsBean.class);
    }

    public ResourceBean getRecommendations(String uid, String keyword, Integer dimension, int limit,AlgorithmOptions algorithmOptions) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.RECOMMENDATIONS;
        String url = getUrl(res, limit, false, keyword);
        if (dimension != null) {
            url += "&dimension=" + dimension;
        }
        if (algorithmOptions != null) {
            String aoString;
            aoString = algorithmOptions.toString();
            if (aoString.length() > 0) {
                url += "&algorithms=" + aoString;
            }
        }
        return getResource(url, ItemsBean.class);
    }

    public ResourceBean getRecommendations(String uid) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.RECOMMENDATIONS;
        String url = getUrl(res);
        return getResource(url, ItemsBean.class);
    }

    public ResourceBean getRecommendedUsers(String userId, String itemId) {
        return getRecommendedUsers(userId, itemId, null);
    }

    public ResourceBean getRecommendedUsers(String userId, String itemId, String linkType) {
        String res = Constants.USERS + "/" + userId + "/recommendations/" + itemId + "/trustgraph";
        String url = getUrl(res);
        if ( linkType != null ) {
            url += "linktype=" + linkType;
        }
        return getResource(url, RecommendedUsersBean.class);
    }

    public ResourceBean getDimensions() {
        String res = Constants.DIMENSIONS;
        String url = getUrl(res);
        return getResource(url, DimensionsBean.class);
    }

    public ResourceBean getDimensionById(String dimensionId) {
        String res = Constants.DIMENSIONS + "/" + dimensionId;
        String url = getUrl(res);
        return getResource(url, DimensionBean.class);
    }

    public ResourceBean getUserActions(String uid, int limit) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.ACTIONS;
        String url = getUrl(res, limit, true, "");
        return getResource(url, ActionsBean.class);
    }

    public ResourceBean getActions() {
        String res = Constants.ACTIONS;
        String url = getUrl(res);
        return getResource(url, ActionsBean.class);
    }

    public ResourceBean getActionById(String actionId) {
        String res = Constants.ACTIONS + "/" + actionId;
        String url = getUrl(res);
        return getResource(url, ActionBean.class);
    }

    @SuppressWarnings({"NullableProblems"})
    @Override
    public ResourceBean getRecommendedUsers(String userId, String itemId, Long linkType, String keywords, Integer limit) {
        String res = Constants.USERS + "/" + userId + "/" + Constants.RECOMMENDATIONS + "/" + itemId + "/" + Constants.TRUSTGRAPH;
        String url = getUrl(res, limit, null, keywords);
        if (linkType != null) {
            url += "&linktype=" + linkType;
        }
        return getResource(url, RecommendedUsersBean.class);
    }

    @Override
    public ResourceBean getItemTypes() {
        String res = Constants.ITEMS + "/" + Constants.TYPES;
        String url = getUrl(res);
        return getResource(url, ItemTypesBean.class);
    }

    @Override
    public ResourceBean getActionTypes() {
        String res = Constants.ACTIONS + "/" + Constants.TYPES;
        String url = getUrl(res);
        return getResource(url, ActionTypesBean.class);
    }

    @Override
    public ResourceBean getRecommendationByItemType(String uid, int itemType, int limit) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.RECOMMENDATIONS;
        String url = getUrl(res) + "type=" + itemType + "&limit=" + limit;
        return getResource(url, ItemsBean.class);
    }

    @Override
    public ResourceBean getUsersMatchingUsername(String username) {
        String url = getUrl(Constants.USERS) + "name=" + username;
        return getResource(url, UsersBean.class);
    }

    // ~~ Post methods ~~

    public Object addAction(ActionBean actionBean) {
        String url = getUrl(Constants.ACTIONS);
        return performPost(url, actionBean);
    }

    public Object addItem(ItemBean itemBean) {
        String url = getUrl(Constants.ITEMS);
        return performPost(url, itemBean);
    }

    @Override
    public Object updateItem(ItemBean itemBean) {
        String url = getUrl(Constants.ITEMS);
        return performPut(url, itemBean);
    }

    @Override
    public Object updateUser(UserBean userBean) {
        return updateUser(userBean, false);
    }

    @Override
    public Object updateUser(UserBean userBean, boolean withSecureConnection) {
        String apiRoot = getApiRoot(withSecureConnection);
        String url = getUrl(apiRoot, Constants.USERS);
        return performPut(url, userBean);
    }

    @Override
    public Object updateAction(ActionBean actionBean) {
        String url = getUrl(Constants.ACTIONS);
        return performPut(url, actionBean);
    }

    @Override
    public Object addUser(UserBean userBean) {
        return addUser(userBean, false);
    }

    @Override
    public Object addUser(UserBean userBean, boolean withSecureConnection) {
        String apiRoot = getApiRoot(withSecureConnection);
        String url = getUrl(apiRoot, Constants.USERS);
        return performPost(url, userBean);
    }

    @SuppressWarnings({"NullableProblems"})
    @Override
    public ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit) {
        return getRankedItems(uid, recs, limit, null);
    }

    @SuppressWarnings({"NullableProblems"})
    @Override
    public ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit, AlgorithmOptions algorithmOptions) {
        String res = Constants.USERS + "/" + uid + "/" + Constants.RECOMMENDATIONS;
        String url = getUrl(res, limit, null, null);
        if (algorithmOptions != null) {
            String aoString;
            aoString = algorithmOptions.toString();
            if (aoString.length() > 0) {
                url += "&algorithms=" + aoString;
            }
        }
        return performPostGet(url, recs, recs.getClass());
    }

    private String getUrl(String resource, Integer limit, Boolean full, String keyword) {
        return getUrl(apiState.getApiUrl(), resource, limit, full, keyword);
    }

    private String getUrl(String baseUrl, String resource, Integer limit, Boolean full, String keyword) {
        String url = baseUrl + "/" + resource + "?";
        if (full != null) {
            url += "full=" + full;
        }
        if (limit != null && limit != Constants.NO_LIMIT) {
            url += "&limit=" + limit;
        }
        if (keyword != null) {
            url += "&keyword=" + keyword;
        }
        return url;
    }

    private String getUrl(String resource, int limit, boolean full, String keyword, String name) {
        String url = getUrl(resource, limit, full, keyword);
        if (name != null) {
            url += ("&name=" + name);
        }
        return url;
    }

    private String getUrl(String resource) {
        return apiState.getApiUrl() + "/" + resource + "?";
    }

    private String getUrl(String apiRoot, String resource) {
        return apiRoot + "/" + resource + "?";
    }

    private String getUrl(String apiRoot, String resource, boolean full) {
        return apiRoot + "/" + resource + "?full=" + full;
    }

    private String getUrl(String resource, boolean full) {
        return apiState.getApiUrl() + "/" + resource + "?full=" + full;
    }

    private ResourceBean getResource(final String url, Class<?> c) {
        logger.info("* GET Endpoint: " + url);
        ResourceBean bean;
        if (token == null) {
            ResourceBean r = getToken();
            if (r instanceof ErrorBean) return r;
        }

        String urlWithToken = url + "&oauth_token=" + token;
        logger.debug("** Token: " + token);
        logger.debug("** Class: " + c.getName());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<String>(headers);
        RestTemplate restTemplate = createRestTemplate();
        try {
            //logger.info("Calling end point: " + urlWithToken + " for class: " + c.getName());
            @SuppressWarnings("unchecked")
            HttpEntity<ResourceBean> res = (HttpEntity<ResourceBean>) restTemplate.exchange(urlWithToken, HttpMethod.GET, entity, c);
            logger.debug("Result: " + res.toString() + " : " + res.getBody());
            bean = res.getBody();
            logger.debug("*** OK (" + urlWithToken + ")");
        }
        catch (Exception e) {
            if ( e.getCause() instanceof SocketTimeoutException ) {
                return createTimeoutBean();
            }
            logger.error("Exception class: " + e.getClass());
            //logger.error("Failed Api Call for url: " + urlWithToken + " : " + e.toString());
            logger.error("*** NOK (" + urlWithToken + "): " + e.getMessage());
            HttpEntity<ErrorBean> res = null;
            try {
                res = restTemplate.exchange(urlWithToken, HttpMethod.GET, entity, ErrorBean.class);
            } catch (RestClientException e1) {
                if ( e1.getCause() instanceof SocketTimeoutException ) {
                    return createTimeoutBean();
                }
            }
            bean = res.getBody();
        }
        if (bean instanceof ErrorBean) {
            if (((ErrorBean) bean).getError_id() != Constants.TOKEN_EXPIRED) {
                return bean;
            } else {
                logger.info("Token expired; fetching a new one.");
                ResourceBean r = getToken();
                if (r instanceof ErrorBean) {
                    return r;
                } else {
                    return getResource(url, c);
                }
            }
        } else {
            return bean;
        }
    }

    /**
     * Use this method to create rest templates -- don't instantiate them directly.
     * 
     * @return a Spring {@link RestTemplate} with the appropriate read timeout.
     */
    private RestTemplate createRestTemplate() {
        RestTemplate restTemplate;
        final CommonsClientHttpRequestFactory requestFactory = new CommonsClientHttpRequestFactory();
        requestFactory.setReadTimeout(readTimeout);
        restTemplate = new RestTemplate(requestFactory);
        return restTemplate;
    }

    private Object performPost(final String endpointUrl, ResourceBean resourceBean) {
        logger.info("* POST Endpoint: " + endpointUrl);
        if (token == null) {
            ResourceBean r = getToken();
            if (r instanceof ErrorBean) return r;
        }
        String url = endpointUrl + "&oauth_token=" + token;
        RestTemplate template = createRestTemplate();
        logger.debug("* Posting: " + resourceBean);
        logger.debug("** Endpoint: " + url);
        // API return types for posts vary: Map on success, ErrorBean on failure -- we're forced to use Object here:
        ResponseEntity<Object> responseEntity = null;
        try {
            responseEntity = template.postForEntity(url, resourceBean, Object.class);
        } catch (ResourceAccessException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                return createTimeoutBean();
            }
        }
        Object body = responseEntity.getBody();
        logger.debug("** Response: " + body);
        if (body instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) body;
            String response = (String) map.get("response");
            String errorMessage = (String) map.get("error_msg"); // if this is an error bean
            if ( response == null && errorMessage != null ) {
                response = errorMessage;
            }
            if (response != null) { // not great; some posts return response maps, some return resource beans
                if (!response.equals("ok")) {
                    if (response.equals("Token expired")) {
                        logger.debug("Token expired; acquiring a fresh one...");
                        ResourceBean r = getToken();
                        if (r instanceof ErrorBean) return r;
                        return performPost(endpointUrl, resourceBean);
                    } else {
                        return new ErrorBean(map);
                    }
                }
            }
        }
        return body;
    }

    private ErrorBean createTimeoutBean() {
        ErrorBean timeoutBean = new ErrorBean();
        timeoutBean.setError_msg("ApiService socket timeout.");
        return timeoutBean;
    }

    private Object performPut(final String endpointUrl, ResourceBean resourceBean) {
        logger.info("* PUT Endpoint: " + endpointUrl);
        if (token == null) {
            ResourceBean r = getToken();
            if (r instanceof ErrorBean) return r;
        }
        String url = endpointUrl + "&oauth_token=" + token;
        RestTemplate template = createRestTemplate();
        logger.debug("* PUTting: " + resourceBean);
        logger.debug("** Endpoint: " + url);
        // API return types for posts vary: Map on success, ErrorBean on failure -- we're forced to use Object here:
        //template.put(url, resourceBean);
        HttpEntity<ResourceBean> entity = new HttpEntity<ResourceBean>(resourceBean);
        ResponseEntity<Object> responseEntity = null;
        try {
            responseEntity = template.exchange(url, HttpMethod.PUT, entity, Object.class);
        } catch (ResourceAccessException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                return createTimeoutBean();
            }
        }
        Object body = responseEntity.getBody();
        logger.debug("PUT response: " + body);
        if (body instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) body;
            String response = (String) map.get("response");
            String errorMessage = (String) map.get("error_msg"); // if this is an error bean
            if (response == null && errorMessage != null) {
                response = errorMessage;
            }
            if (response != null) { // not great; some posts return response maps, some return resource beans
                if (!response.equals("ok")) {
                    if (response.equals("Token expired")) {
                        logger.debug("Token expired; acquiring a fresh one...");
                        ResourceBean r = getToken();
                        if (r instanceof ErrorBean) return r;
                        return performPut(endpointUrl, resourceBean);
                    } else {
                        return new ErrorBean(map);
                    }
                }
            }
        }
        return body;
    }

    private ResourceBean performPostGet(final String endpointUrl, final ResourceBean resourceBean, Class c) {
        ResourceBean bean;
        if (token == null) {
            ResourceBean r = getToken();
            if (r instanceof ErrorBean) return r;
        }
        String url = endpointUrl + "&oauth_token=" + token;
        //header
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<ResourceBean> entity = new HttpEntity<ResourceBean>(resourceBean, headers);
        RestTemplate template = createRestTemplate();
        try {
            logger.debug("* Posting: " + resourceBean);
            logger.debug("** Endpoint: " + url);
            // API return types for posts vary: Map on success, ErrorBean on failure -- we're forced to use Object here:
            ResponseEntity<ResourceBean> responseEntity;
            responseEntity = template.postForEntity(url, entity, c);
            bean = responseEntity.getBody();
        } catch (Exception e) {
            if ( e.getCause() instanceof SocketTimeoutException ) {
                return createTimeoutBean();
            }
            logger.error("Exception class: " + e.getClass());
            //logger.error("Failed Api Call for url: " + urlWithToken + " : " + e.toString());
            logger.error("*** NOK (" + url + "): " + e.getMessage());
            HttpEntity<ErrorBean> res;
            try {
                res = template.postForEntity(url, entity, ErrorBean.class);
            } catch (ResourceAccessException rae) {
                return createTimeoutBean();
            }
            bean = res.getBody();
        }
        if (bean instanceof ErrorBean) {
            if (((ErrorBean) bean).getError_id() != Constants.TOKEN_EXPIRED) {
                return bean;
            } else {
                logger.info("Token expired; fetching a new one.");
                ResourceBean r = getToken();
                if (r instanceof ErrorBean) {
                    return r;
                } else {
                    return performPostGet(endpointUrl, resourceBean, c);
                }
            }
        } else {
            return bean;
        }
    }


    private ResourceBean getTokenResource(String url) {
        ResourceBean bean;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<String>(headers);
        try {
            ResponseEntity<TokenBean> res = new RestTemplate().exchange(url, HttpMethod.GET, entity, TokenBean.class);
            bean = res.getBody();
        } catch (Exception e) {
            HttpEntity<ErrorBean> res = new RestTemplate().exchange(url, HttpMethod.GET, entity, ErrorBean.class);
            bean = res.getBody();
        }
        return bean;
    }

	

}
