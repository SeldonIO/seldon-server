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
package io.seldon.client;

import io.seldon.client.algorithm.AlgorithmOptions;
import io.seldon.client.beans.ActionBean;
import io.seldon.client.beans.ActionTypeBean;
import io.seldon.client.beans.ActionTypesBean;
import io.seldon.client.beans.ActionsBean;
import io.seldon.client.beans.DimensionBean;
import io.seldon.client.beans.DimensionsBean;
import io.seldon.client.beans.ErrorBean;
import io.seldon.client.beans.ItemBean;
import io.seldon.client.beans.ItemGraphBean;
import io.seldon.client.beans.ItemSimilarityNodeBean;
import io.seldon.client.beans.ItemTypeBean;
import io.seldon.client.beans.ItemTypesBean;
import io.seldon.client.beans.ItemsBean;
import io.seldon.client.beans.OpinionBean;
import io.seldon.client.beans.OpinionsBean;
import io.seldon.client.beans.RecommendationBean;
import io.seldon.client.beans.RecommendationsBean;
import io.seldon.client.beans.RecommendedUserBean;
import io.seldon.client.beans.RecommendedUsersBean;
import io.seldon.client.beans.ResourceBean;
import io.seldon.client.beans.UserBean;
import io.seldon.client.beans.UserGraphBean;
import io.seldon.client.beans.UserTrustNodeBean;
import io.seldon.client.beans.UsersBean;
import io.seldon.client.exception.ApiException;
import io.seldon.client.services.ApiService;
import io.seldon.client.services.ApiServiceImpl;
import io.seldon.client.services.ApiState;

import java.util.List;

/**
 * The default Seldon API client implementation.
 */
@SuppressWarnings({"NullableProblems"})
public class DefaultApiClient implements ApiClient {

    private ApiService apiService;

    /**
     * Create a new API client instance with the supplied credentials.
     *
     * @param apiUrl         if you've been provided access to additional API installations, supply the URL here
     * @param consumerKey    your consumer key
     * @param consumerSecret your consumer secret
     */
    public DefaultApiClient(String apiUrl, String consumerKey, String consumerSecret) {
        setupService(apiUrl, null, consumerKey, consumerSecret, null);
    }

    /**
     * Create a new API client instance with the supplied credentials.
     *
     * @param apiUrl         if you've been provided access to additional API installations, supply the URL here
     * @param secureApiUrl   secure API url (if provided)
     * @param consumerKey    your consumer key
     * @param consumerSecret your consumer secret
     */
    public DefaultApiClient(String apiUrl, String secureApiUrl, String consumerKey, String consumerSecret) {
        setupService(apiUrl, secureApiUrl, consumerKey, consumerSecret, null);
    }

    /**
     * Create a new API client instance with the supplied credentials.
     *
     * @param apiUrl         if you've been provided access to additional API installations, supply the URL here
     * @param consumerKey    your consumer key
     * @param consumerSecret your consumer secret
     * @param readTimeout    socket timeout for client (0 is unlimited).
     */
    public DefaultApiClient(String apiUrl, String consumerKey, String consumerSecret, Integer readTimeout) {
        setupService(apiUrl, null, consumerKey, consumerSecret, readTimeout);
    }

    /**
     * Create a new API client instance with the supplied credentials.
     *
     * @param apiUrl         if you've been provided access to additional API installations, supply the URL here
     * @param secureApiUrl   secure API url (if provided)
     * @param consumerKey    your consumer key
     * @param consumerSecret your consumer secret
     * @param readTimeout    socket timeout for client (0 is unlimited).
     */
    public DefaultApiClient(String apiUrl, String secureApiUrl, String consumerKey, String consumerSecret, Integer readTimeout) {
        setupService(apiUrl, secureApiUrl, consumerKey, consumerSecret, readTimeout);
    }

    private void setupService(String apiUrl, String secureApiUrl, String consumerKey, String consumerSecret, Integer readTimeout) {
        ApiState state = setupState(apiUrl, secureApiUrl, consumerKey, consumerSecret, readTimeout);
        ApiServiceImpl apiServiceImpl = new ApiServiceImpl();
        apiServiceImpl.setApiState(state);
        apiService = apiServiceImpl;
    }

    private ApiState setupState(String apiUrl, String secureApiUrl, String consumerKey, String consumerSecret, Integer readTimeout) {
        ApiState state = new ApiState();
        state.setApiUrl(apiUrl);
        //String secureUrl = (secureApiUrl ==  null) ? apiUrl : secureApiUrl;
        //state.setSecureApiUrl(secureUrl);
        state.setSecureApiUrl(secureApiUrl);
        state.setConsumerKey(consumerKey);
        state.setConsumerSecret(consumerSecret);
        if ( readTimeout != null ) {
            state.setReadTimeout(readTimeout);
        }
        return state;
    }

    @SuppressWarnings("unchecked")
    private <T> T retrieveResource(ResourceBean bean, Class<T> clazz) throws ApiException {
        if (bean.getClass().equals(clazz)) {
            return (T) bean;
        } else {
            throw new ApiException(bean);
        }
    }

    @Override
    public List<ItemBean> getItems(int limit, boolean full, String keyword, String name) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItems(limit, full, keyword, name);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
        return itemsBean.getList();
    }

    @Override
    public List<ItemBean> getItems(int limit, Integer itemType, boolean full, String keyword, String name) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItems(limit, itemType, full, keyword, name);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
        return itemsBean.getList();
    }

    @Override
    public List<ItemBean> getItems(int limit, boolean full) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItems(limit, full);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
        return itemsBean.getList();
    }

    @Override
    public List<ItemBean> getItems(int limit, int type, boolean full) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItems(limit, type, full);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
        return itemsBean.getList();
    }
    
    @Override
	public List<ItemBean> getItems(int limit, Integer itemType, boolean full,
			String sorting) throws ApiException {
    	 ResourceBean resourceBean;
         try {
             resourceBean = apiService.getItems(limit, itemType, full, sorting);
         } catch (Throwable e) {
             throw new ApiException(e);
         }
         ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
         return itemsBean.getList();
	}

	@Override
	public List<ItemBean> getItems(int limit, boolean full, String sorting) throws ApiException {
		ResourceBean resourceBean = apiService.getItems(limit, full, sorting);
	    ItemsBean itemsBean = retrieveResource(resourceBean, ItemsBean.class);
	    return itemsBean.getList();
	}
    
    @Override
    public ItemBean getItem(String id, boolean full) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItem(id, full);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        return retrieveResource(resourceBean, ItemBean.class);
    }

    @Override
    public List<UserBean> getUsers(int limit, boolean full) throws ApiException {
        return getUsers(limit, full, false);
    }

    @Override
    public List<UserBean> getUsers(int limit, boolean full, boolean withSecureConnection) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getUsers(limit, full, withSecureConnection);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        UsersBean usersBean = retrieveResource(resourceBean, UsersBean.class);
        return usersBean.getList();
    }

    @Override
    public UserBean getUser(String id, boolean full) throws ApiException {
        return getUser(id, full, false);
    }

    @Override
    public UserBean getUser(String id, boolean full, boolean withSecureConnection) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getUser(id, full, withSecureConnection);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        return retrieveResource(resourceBean, UserBean.class);
    }

    @Override
    public List<ItemSimilarityNodeBean> getSimilarItems(String id, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getSimilarItems(id, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemGraphBean itemsBeans = retrieveResource(resourceBean, ItemGraphBean.class);
        return itemsBeans.getList();
    }

    @Override
    public List<UserTrustNodeBean> getTrustedUsers(String id, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getTrustedUsers(id, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        UserGraphBean usersBean = retrieveResource(resourceBean, UserGraphBean.class);
        return usersBean.getList();
    }

    @Override
    public OpinionBean getPrediction(String uid, String oid) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getPrediction(uid, oid);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        return retrieveResource(resourceBean, OpinionBean.class);
    }

    @Override
    public List<OpinionBean> getOpinions(String uid, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getOpinions(uid, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        OpinionsBean opinionsBean = retrieveResource(resourceBean, OpinionsBean.class);
        return opinionsBean.getList();
    }

    @Override
    public List<OpinionBean> getItemOpinions(String itemId, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItemOpinions(itemId, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        OpinionsBean opinionsBean = retrieveResource(resourceBean, OpinionsBean.class);
        return opinionsBean.getList();
    }

    @Override
    public List<ItemBean> getRecommendations(String uid, String keyword, Integer dimension, int limit) throws ApiException {
       return getRecommendations(uid, keyword, dimension, limit, null);
    }

    @Override
    public List<ItemBean> getRecommendations(String uid, String keyword, Integer dimension, int limit,AlgorithmOptions algorithmOptions) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getRecommendations(uid, keyword, dimension, limit,algorithmOptions);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean recommendationsBean = retrieveResource(resourceBean, ItemsBean.class);
        return recommendationsBean.getList();
    }

    @Override
    public List<ItemBean> getRecommendations(String uid, String keyword, int limit) throws ApiException {
        return getRecommendations(uid, keyword, null, limit);
    }

    @Override
    public List<ItemBean> getRecommendations(String uid, Integer dimension, int limit) throws ApiException {
        return getRecommendations(uid, null, dimension, limit);
    }

    @Override
    public List<ItemBean> getRecommendations(String uid) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getRecommendations(uid);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemsBean recommendationsBean = retrieveResource(resourceBean, ItemsBean.class);
        return recommendationsBean.getList();
    }

    @Override
    public List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId) throws ApiException {
        return getRecommendedUsers(userId, itemId, null);
    }

    @Override
    public List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId, String linkType) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getRecommendedUsers(userId, itemId, linkType);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        RecommendedUsersBean recommendedUsersBean = retrieveResource(resourceBean, RecommendedUsersBean.class);
        return recommendedUsersBean.getList();
    }

    @Override
    public List<DimensionBean> getDimensions() throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getDimensions();
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        DimensionsBean dimensionsBean = retrieveResource(resourceBean, DimensionsBean.class);
        return dimensionsBean.getList();
    }

    @Override
    public DimensionBean getDimensionById(String dimensionId) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getDimensionById(dimensionId);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        return retrieveResource(resourceBean, DimensionBean.class);
    }

    @Override
    public List<ActionBean> getUserActions(String uid, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getUserActions(uid, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ActionsBean actionsBean = retrieveResource(resourceBean, ActionsBean.class);
        return actionsBean.getList();
    }

    @Override
    public List<ActionBean> getActions() throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getActions();
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ActionsBean actionsBean = retrieveResource(resourceBean, ActionsBean.class);
        return actionsBean.getList();
    }

    @Override
    public List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId,
                                                         Long linkType, String keywords,
                                                         Integer limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getRecommendedUsers(userId, itemId, linkType, keywords, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        RecommendedUsersBean recommendedUsersBean = retrieveResource(resourceBean, RecommendedUsersBean.class);
        return recommendedUsersBean.getList();
    }

    @Override
    public ActionBean getActionById(String actionId) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getActionById(actionId);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        return retrieveResource(resourceBean, ActionBean.class);
    }

    @Override
    public List<ItemTypeBean> getItemTypes() throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getItemTypes();
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ItemTypesBean itemTypesBean = retrieveResource(resourceBean, ItemTypesBean.class);
        return itemTypesBean.getList();
    }

    @Override
    public List<ActionTypeBean> getActionTypes() throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getActionTypes();
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        ActionTypesBean actionTypesBean = retrieveResource(resourceBean, ActionTypesBean.class);
        return actionTypesBean.getList();
    }


    @Override
    public List<RecommendationBean> getRecommendationsByType(String uid, int itemType, int limit) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getRecommendationByItemType(uid, itemType, limit);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        RecommendationsBean recommendationsBean = retrieveResource(resourceBean, RecommendationsBean.class);
        return recommendationsBean.getList();
    }

    @Override
    public List<UserBean> getUsersMatchingUsername(String username) throws ApiException {
        ResourceBean resourceBean;
        try {
            resourceBean = apiService.getUsersMatchingUsername(username);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        UsersBean usersBean = retrieveResource(resourceBean, UsersBean.class);
        return usersBean.getList();
    }

    @Override
    public ActionBean addAction(ActionBean actionBean) throws ApiException {
        Object response;
        try {
            response = apiService.addAction(actionBean);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return actionBean;
    }

    @Override
    public ItemBean addItem(ItemBean itemBean) throws ApiException {
        Object response;
        try {
            response = apiService.addItem(itemBean);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return itemBean;
    }

    @Override
    public ItemBean updateItem(ItemBean itemBean) throws ApiException {
        Object response;
        try {
            response = apiService.updateItem(itemBean);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return itemBean;
    }

    @Override
    public UserBean addUser(UserBean userBean) throws ApiException {
        return addUser(userBean, false);
    }

    @Override
    public UserBean addUser(UserBean userBean, boolean withSecureConnection) throws ApiException {
        Object response;
        try {
            response = apiService.addUser(userBean, withSecureConnection);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return userBean;
    }

    @Override
    public UserBean updateUser(UserBean userBean) throws ApiException {
        return updateUser(userBean, false);
    }

    @Override
    public UserBean updateUser(UserBean userBean, boolean withSecureConnection) throws ApiException {
        Object response;
        try {
            response = apiService.updateUser(userBean, withSecureConnection);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return userBean;
    }

    @Override
    public ActionBean updateAction(ActionBean actionBean) throws ApiException {
        Object response;
        try {
            response = apiService.updateAction(actionBean);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException((ResourceBean) response);
        }
        return actionBean;
    }

    @Override
    public List<RecommendationBean> getRankedItems(String uid, List<RecommendationBean> recommendations, Integer limit)
            throws ApiException {
        RecommendationsBean recs = new RecommendationsBean();
        recs.setList(recommendations);
        RecommendationsBean recommendationsBean = getRankedItems(uid, recs, limit);
        return recommendationsBean.getList();
    }

    @Override
    public List<RecommendationBean> getRankedItems(String uid, List<RecommendationBean> recommendations, Integer limit,
                                                   AlgorithmOptions algorithmOptions) throws ApiException {
        RecommendationsBean recs = new RecommendationsBean();
        recs.setList(recommendations);
         RecommendationsBean recommendationsBean = getRankedItems(uid, recs, limit, algorithmOptions);
        return recommendationsBean.getList();
    }

    @Override
    public RecommendationsBean getRankedItems(String uid, RecommendationsBean recs, Integer limit) throws ApiException {
        return getRankedItems(uid, recs, limit, null);
    }

    @Override
    public RecommendationsBean getRankedItems(String uid, RecommendationsBean recs, Integer limit,
                                              AlgorithmOptions algorithmOptions) throws ApiException {
        ResourceBean response;
        try {
            response = apiService.getRankedItems(uid, recs, limit, algorithmOptions);
        } catch (Throwable e) {
            throw new ApiException(e);
        }
        if (response instanceof ErrorBean) {
            throw new ApiException(response);
        } else if (response instanceof RecommendationsBean) {
            return (RecommendationsBean) response;
        } else return recs;
    }

	

}
