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

/**
 * Created by: marc on 12/08/2011 at 10:02
 */
public interface ApiService {

    ResourceBean getToken();

    ResourceBean getItems(int limit, boolean full, String keyword, String name);

    ResourceBean getItems(int limit, boolean full);
    
    ResourceBean getItems(int limit, boolean full, String sorting);

    ResourceBean getItems(int limit, int type, boolean full);

    ResourceBean getItems(int limit, Integer itemType, boolean full, String keyword, String name);

    ResourceBean getItems(int limit, Integer itemType, boolean full,String sorting);
    
    ResourceBean getItem(String id, boolean full);

    ResourceBean getUsers(int limit, boolean full);
    ResourceBean getUsers(int limit, boolean full, boolean withSecureConnection);

    ResourceBean getUser(String id, boolean full);
    ResourceBean getUser(String id, boolean full, boolean withSecureConnection);

    ResourceBean getSimilarItems(String id, int limit);

    ResourceBean getTrustedUsers(String id, int limit);

    ResourceBean getPrediction(String uid, String oid);

    ResourceBean getOpinions(String uid, int limit);

    ResourceBean getItemOpinions(String itemId, int limit);

    ResourceBean getRecommendations(String uid, String keyword, Integer dimension, int limit,AlgorithmOptions algorithmOptions);

    ResourceBean getRecommendations(String uid);

    public ResourceBean getRecommendedUsers(String userId, String itemId);

    public ResourceBean getRecommendedUsers(String userId, String itemId, String linkType);

    ResourceBean getRecommendationByItemType(String uid, int itemType, int limit);

    ResourceBean getDimensions();

    ResourceBean getDimensionById(String dimensionId);

    ResourceBean getUserActions(String uid, int limit);

    ResourceBean getActions();

    ResourceBean getActionById(String actionId);
    
    ResourceBean getRecommendedUsers(String userId, String itemId, Long linkType, String keywords,Integer limit);
    
    ResourceBean getItemTypes();
    
    ResourceBean getActionTypes();

    ResourceBean getUsersMatchingUsername(String username);

    Object addAction(ActionBean actionBean);

    Object addItem(ItemBean itemBean);

    Object addUser(UserBean userBean);
    Object addUser(UserBean userBean, boolean withSecureConnection);

    Object updateItem(ItemBean itemBean);

    Object updateUser(UserBean userBean);
    Object updateUser(UserBean userBean, boolean withSecureConnection);

    Object updateAction(ActionBean actionBean);
    
    ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit);

    ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit, AlgorithmOptions algorithmOptions);

}
