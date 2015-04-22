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
package io.seldon.client.test;

import io.seldon.client.beans.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Superseded by {@link ApiClientTest}
 *
 * Created by: marc on 05/08/2011 at 17:42
 */
@Ignore
public class ApiBeansTest extends BaseBeansTest {

    private static final Integer USER_LIMIT = 10;
    private static final Integer ITEM_LIMIT = 10;
    private static final Integer USER_ACTION_LIMIT = 10;
    private static final Integer USER_RECOMMENDATIONS_LIMIT = 10;
    private static final Integer DIMENSIONS_LIMIT = 15;

    private List<UserBean> users;
    private List<ItemBean> items;
    private List<DimensionBean> dimensions;
    private List<ItemTypeBean> itemTypes;
    private List<ActionTypeBean> actionTypes;

    /**
     * Users, items and dimensions are made available to all tests.
     * <p/>
     * The endpoints used to retrieve these data are tested separately below; should retrieval fail here,
     * we simply create empty lists.
     */
    @Before
    public void setup() {
        setupUsers();
        setupItems();
        setupDimensions();
        setupActionTypes();
        setupItemTypes();
    }

    private void setupDimensions() {
        try {
            this.dimensions = getDimensions();
        } catch (ErrorBeanException e) {
            this.dimensions = new LinkedList<DimensionBean>();
        }
    }

    private void setupItems() {
        try {
            ItemsBean itemsBean = (ItemsBean) checkedResource(apiService.getItems(ITEM_LIMIT, false));
            this.items = itemsBean.getList();
        } catch (ErrorBeanException e) {
            logger.error("Couldn't retrieve items.");
            this.items = new LinkedList<ItemBean>();
        }
    }

    private void setupUsers() {
        try {
            UsersBean usersBean = (UsersBean) checkedResource(apiService.getUsers(USER_LIMIT, false));
            this.users = usersBean.getList();
        } catch (ErrorBeanException e) {
            logger.error("Couldn't retrieve users.");
            this.users = new LinkedList<UserBean>();
        }
    }
    
    private void setupItemTypes() {
    	try {
    		ItemTypesBean itemTypesBean = (ItemTypesBean) checkedResource(apiService.getItemTypes());
    		this.itemTypes = itemTypesBean.getList();
    	}
    	catch(ErrorBeanException e) {
    		logger.error("Couldn't retrieve item types.");
            this.itemTypes = new LinkedList<ItemTypeBean>();
    	}
     }
    
    private void setupActionTypes() {
    	try {
    		ActionTypesBean actionTypesBean = (ActionTypesBean) checkedResource(apiService.getActionTypes());
    		this.actionTypes = actionTypesBean.getList();
    	}
    	catch(ErrorBeanException e) {
    		logger.error("Couldn't retrieve action types.");
            this.actionTypes = new LinkedList<ActionTypeBean>();
    	}
    }

    @Test
    public void retrieveUsers() throws ErrorBeanException {
        UsersBean usersBean = (UsersBean) checkedResource(apiService.getUsers(USER_LIMIT, false));
        List<UserBean> userBeans = usersBean.getList();
        logUserInfo(userBeans);
    }

    @Test
    public void retrieveUsersFull() throws ErrorBeanException {
        UsersBean usersBean = (UsersBean) checkedResource(apiService.getUsers(USER_LIMIT, true));
        List<UserBean> userBeans = usersBean.getList();
        logUserInfo(userBeans);
    }

    @Test
    public void retrieveUsersById() throws ErrorBeanException {
        for (UserBean user : users) {
            String userId = user.getId();
            UserBean retrievedUser = (UserBean) checkedResource(apiService.getUser(userId, false));
            logger.debug("Retrieved: " + retrievedUser);
        }
    }

    @Test
    public void retrieveUsersFullById() throws ErrorBeanException {
        for (UserBean user : users) {
            String userId = user.getId();
            UserBean retrievedUser = (UserBean) checkedResource(apiService.getUser(userId, true));
            logger.debug("Retrieved: " + retrievedUser);
        }
    }

    @Test
    public void retrieveItems() throws ErrorBeanException {
        ItemsBean itemsBean = (ItemsBean) checkedResource(apiService.getItems(ITEM_LIMIT, false));
        List<ItemBean> itemsBeanList = itemsBean.getList();
        logItemInfo(itemsBeanList);
    }

    @Test
    public void retrieveItemsFull() throws ErrorBeanException {
        ItemsBean itemsBean = (ItemsBean) checkedResource(apiService.getItems(ITEM_LIMIT, true));
        List<ItemBean> itemsBeanList = itemsBean.getList();
        logItemInfo(itemsBeanList);
    }

    @Test
    public void retrieveItemsById() throws ErrorBeanException {
        for (ItemBean item : items) {
            String itemId = item.getId();
            ItemBean itemBean = (ItemBean) checkedResource(apiService.getItem(itemId, false));
            logger.debug("Item: " + itemBean);
        }
    }

    @Test
    public void retrieveItemsFullById() throws ErrorBeanException {
        for (ItemBean item : items) {
            String itemId = item.getId();
            ItemBean itemBean = (ItemBean) checkedResource(apiService.getItem(itemId, true));
            logger.debug("Item: " + itemBean);
        }
    }

    @Test
    public void retrieveDimensions() throws ErrorBeanException {
        getDimensions();
    }

    @Test
    public void retrieveDimensionsById() throws ErrorBeanException {
        //List<DimensionBean> dimensions = getDimensions();
        for (DimensionBean knownDimension : dimensions) {
            String dimensionId = String.valueOf(knownDimension.getDimId());
            DimensionBean dimensionBean = (DimensionBean) checkedResource(apiService.getDimensionById(dimensionId));
            logger.debug("Dimension: " + dimensionBean);
        }
    }

    private List<DimensionBean> getDimensions() throws ErrorBeanException {
        DimensionsBean dimensionsBean = (DimensionsBean) checkedResource(apiService.getDimensions());
        List<DimensionBean> dimensions = dimensionsBean.getList();
        for (DimensionBean dimension : dimensions) {
            logger.debug("Dimension: " + dimension);
        }
        return dimensions;
    }

    @Test
    public void retrieveActionsForUsers() throws ErrorBeanException {
        for (UserBean user : users) {
            String userId = user.getId();
            ActionsBean actions = (ActionsBean) checkedResource(apiService.getUserActions(userId, USER_ACTION_LIMIT));
            List<ActionBean> actionsList = actions.getList();
            for (ActionBean actionBean : actionsList) {
                logger.debug("Action: " + actionBean);
            }
        }
    }

    @Test
    public void retrieveActions() throws ErrorBeanException {
        ActionsBean actions = (ActionsBean) checkedResource(apiService.getActions());
        List<ActionBean> actionsList = actions.getList();
        for (ActionBean actionBean : actionsList) {
            logger.debug("Action: " + actionBean);
        }
    }

    @Test
    public void retrieveActionsById() throws ErrorBeanException {
        ActionsBean actions = (ActionsBean) checkedResource(apiService.getActions());
        List<ActionBean> actionsList = actions.getList();
        for (ActionBean actionBean : actionsList) {
            String actionId = String.valueOf(actionBean.getActionId());
            ActionBean individualActionBean = (ActionBean) checkedResource(apiService.getActionById(actionId));
            logger.debug("Action: " + individualActionBean);
        }
    }

    @Test
    public void retrieveRecommendationsForUser() throws ErrorBeanException {
        for (UserBean user : users) {
            String userId = user.getId();
            ItemsBean recommendationsBean = (ItemsBean) checkedResource(apiService.getRecommendations(userId));
            List<ItemBean> recommendations = recommendationsBean.getList();
            for (ItemBean recommendation : recommendations) {
                logger.debug("Recommendation for user: " + userId + " => " + recommendation);
            }
        }
    }

    @Test
    public void retrieveRecommendationsWithDimensionsForUser() throws ErrorBeanException {
        for (UserBean user : users) {
            String userId = user.getId();
            int limit = Math.min(dimensions.size(), DIMENSIONS_LIMIT);
            for (DimensionBean dimension : dimensions.subList(0, limit)) {
                int dimensionId = dimension.getDimId();
                @SuppressWarnings({"NullableProblems"})
                ItemsBean recommendationsBean = (ItemsBean) checkedResource(
                        apiService.getRecommendations(userId, null, dimensionId, USER_RECOMMENDATIONS_LIMIT,null)
                );
                List<ItemBean> recommendations = recommendationsBean.getList();
                for (ItemBean recommendation : recommendations) {
                    logger.debug("Recommendation for user: " + userId + " => in dimension: " + dimension + " => " + recommendation);
                }
            }
        }
    }

    @Test
    public void retrieveItemTypes() throws ErrorBeanException {
    	ItemTypesBean itemTypes = (ItemTypesBean) checkedResource(apiService.getItemTypes());
        List<ItemTypeBean> itemTypesList = itemTypes.getList();
        for (ItemTypeBean itemTypeBean : itemTypesList) {
            logger.debug("ItemType: " + itemTypeBean);
        }
    }
    
    @Test
    public void retrieveActionTypes() throws ErrorBeanException {
    	ActionTypesBean actionTypes = (ActionTypesBean) checkedResource(apiService.getActionTypes());
        List<ActionTypeBean> actionTypesList = actionTypes.getList();
        for (ActionTypeBean actionTypeBean : actionTypesList) {
            logger.debug("actionType: " + actionTypeBean);
        }
    }

    @Test
    public void retrieveRecommendationByType() throws ErrorBeanException {
    	for(ItemTypeBean t : itemTypes) {
    		 for (UserBean user : users) {
    	            String userId = user.getId();
    	            ItemsBean recommendationsBean = (ItemsBean) checkedResource(apiService.getRecommendationByItemType(userId, t.getTypeId(), USER_RECOMMENDATIONS_LIMIT));
    	            List<ItemBean> recommendations = recommendationsBean.getList();
    	            for (ItemBean recommendation : recommendations) {
    	                logger.debug("RecommendationByType"+ t.getName() + " for user: " + userId + " => " + recommendation);
    	            }
    	        }
    	}
    	
    }
    
    private void logUserInfo(List<UserBean> userBeans) {
        for (UserBean userBean : userBeans) {
            logger.debug(userBean);
        }
    }

    private void logItemInfo(List<ItemBean> itemsBeanList) {
        for (ItemBean itemBean : itemsBeanList) {
            logger.debug(itemBean);
        }
    }

}
