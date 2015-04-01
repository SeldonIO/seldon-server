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
import io.seldon.client.beans.DimensionBean;
import io.seldon.client.beans.ItemBean;
import io.seldon.client.beans.ItemSimilarityNodeBean;
import io.seldon.client.beans.ItemTypeBean;
import io.seldon.client.beans.OpinionBean;
import io.seldon.client.beans.RecommendationBean;
import io.seldon.client.beans.RecommendationsBean;
import io.seldon.client.beans.RecommendedUserBean;
import io.seldon.client.beans.ResourceBean;
import io.seldon.client.beans.UserBean;
import io.seldon.client.beans.UserTrustNodeBean;
import io.seldon.client.exception.ApiException;

import java.util.List;

/**
 * Seldon IO API client.
 * <p/>
 * Please see {@link DefaultApiClient} for the default implementation.
 */
public interface ApiClient {

    /**
     * Retrieve items filtered by keyword and/or name
     *
     * @param limit   The maximum number of items to retrieve
     * @param full    If true, include item attributes and demographics
     * @param keyword Optional comma-separate keyword string (<code>null</code> can be supplied to disable
     *                <i>keyword</i> keyword filtering)
     * @param name    Optionally filter items by name (<code>null</code> can be supplied to disable
     *                <i>name</i> filtering).
     * @return a {@link List list} of items
     * @throws ApiException in the event of an API error.
     */
    List<ItemBean> getItems(int limit, boolean full, String keyword, String name) throws ApiException;

    List<ItemBean> getItems(int limit, Integer itemType, boolean full, String keyword, String name) throws ApiException;


    /**
     * Retrieve items
     *
     * @param limit The maximum number of items to retrieve.
     * @param full  If true, include item attributes and demographics
     * @return an {@link List list} of item beans
     * @throws ApiException in the event of an API error.
     */
    List<ItemBean> getItems(int limit, boolean full) throws ApiException;
    
    List<ItemBean> getItems(int limit, boolean full, String sorting) throws ApiException;
    
    /**
     * Retrieve items
     * 
     * @param limit The maximum number of items to retrieve.
     * @param itemType The type of items to retrieve
     * @param full  If true, include item attributes and demographics
     * @param sorting The sorting to use
     * @return an {@link List list} of item beans
     * @throws ApiException
     */
    List<ItemBean> getItems(int limit, Integer itemType, boolean full,String sorting) throws ApiException;

    /**
     * Retrieve items of a given type
     *
     * @param limit The maximum number of items to retrieve.
     * @param type  The type of items to retrieve
     * @param full  If true, include item attributes and demographics
     * @return an {@link List list} of item beans
     * @throws ApiException in the event of an API error.
     */
    List<ItemBean> getItems(int limit, int type, boolean full) throws ApiException;


    /**
     * Retrieve an item with the supplied ID.
     *
     * @param id   The item's ID.
     * @param full If true, include item attributes and demographics
     * @return an item bean
     * @throws ApiException in the event of an API error.
     */
    ItemBean getItem(String id, boolean full) throws ApiException;

    /**
     * Retrieve users
     *
     * @param limit The maximum number of users to retrieve
     * @param full  If true, include item attributes and demographics
     * @return a {@link List list} of users
     * @throws ApiException in the event of an API error.
     */
    List<UserBean> getUsers(int limit, boolean full) throws ApiException;

    List<UserBean> getUsers(int limit, boolean full, boolean withSecureConnection) throws ApiException;

    /**
     * Retrieve a user with the supplied ID.
     *
     * @param id   The user's ID
     * @param full If true, also retrieve user attributes and dimensions
     * @return a user bean
     * @throws ApiException in the event of an API error.
     */
    UserBean getUser(String id, boolean full) throws ApiException;

    UserBean getUser(String id, boolean full, boolean withSecureConnection) throws ApiException;

    /**
     * Retrieve a list of items similar to the supplied item.
     *
     * @param id    The item's ID
     * @param limit The maximum number of nodes to retrieve
     * @return A list of nodes constituting the graph.
     * @throws ApiException in the event of an API error.
     */
    List<ItemSimilarityNodeBean> getSimilarItems(String id, int limit) throws ApiException;

    /**
     * Retrieve the a list of trusted users for the supplied user.
     *
     * @param id    The user's ID
     * @param limit The maximum number of nodes to retrieve
     * @return A list of nodes constituting the graph.
     * @throws ApiException in the event of an API error.
     */
    List<UserTrustNodeBean> getTrustedUsers(String id, int limit) throws ApiException;

    /**
     * If the user already has an opinion of the given item, this method will return it.
     * <p/>
     * Otherwise, it will return a prediction on the basis of past user behaviour.
     *
     * @param uid A user ID
     * @param oid An item ID
     * @return An opinion bean
     * @throws ApiException in the event of an API error.
     */
    OpinionBean getPrediction(String uid, String oid) throws ApiException;

    /**
     * Retrieve a set of opinions for the supplied user.
     *
     * @param uid   The user's ID.
     * @param limit The maxmimum number of opinions to retrieve.
     * @return a {@link List list} of opinions
     * @throws ApiException in the event of an API error.
     */
    List<OpinionBean> getOpinions(String uid, int limit) throws ApiException;

    /**
     * Retrieve a set of opinions for the supplied item.
     *
     * @param itemId The item's ID
     * @param limit  The maximum number of opinions to retrieve
     * @return a {@link List list} of opinions
     * @throws ApiException in the event of an API error.
     */
    List<OpinionBean> getItemOpinions(String itemId, int limit) throws ApiException;

    /**
     * Get recommendations for the supplied user optionally filtered by keyword and dimension.
     *
     * @param uid       The user's ID.
     * @param keyword   An optional comma-separated keyword list {@link String string}.
     *                  (Pass in <code>null</code> for no keyword filtering or use another
     *                  ApiClient#getRecommendations overload)
     * @param dimension Filter by dimension ID
     *                  <ul>
     *                  <li>
     *                  if <code>-1</code>, recommendations are based on <i>trust</i>;
     *                  </li>
     *                  <li>
     *                  if <code>0</code> recommendations
     *                  are based on <i>trust</i> and <i>user dimensions</i>;
     *                  </li>
     *                  <li>
     *                  if <code>&gt;1</code>, filter by specific dimension ID.
     *                  </li>
     *                  </ul>
     * @param limit     The maximum number of recommendations to retrieve
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendations(String uid, String keyword, Integer dimension, int limit) throws ApiException;

    /**
     * Get recommendations for the supplied user optionally filtered by keyword and dimension.
     *
     * @param uid     The user's ID.
     * @param keyword An optional comma-separated keyword list {@link String string}.
     *                (Pass in <code>null</code> for no keyword filtering.)
     * @param limit   The maximum number of recommendations to retrieve
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendations(String uid, String keyword, int limit) throws ApiException;

    /**
     * Get recommendations for the supplied user optionally filtered by keyword and dimension.
     *
     * @param uid       The user's ID.
     * @param dimension Filter by dimension ID
     *                  <ul>
     *                  <li>
     *                  if <code>-1</code>, recommendations are based on <i>trust</i>;
     *                  </li>
     *                  <li>
     *                  if <code>0</code> recommendations
     *                  are based on <i>trust</i> and <i>user dimensions</i>;
     *                  </li>
     *                  <li>
     *                  if <code>&gt;1</code>, filter by specific dimension ID.
     *                  </li>
     *                  </ul>
     * @param limit     The maximum number of recommendations to retrieve
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendations(String uid, Integer dimension, int limit) throws ApiException;

    /**
     * Get recommendations for the supplied user optionally filtered by keyword and dimension.
     *
     * @param uid       The user's ID.
     * @param dimension Filter by dimension ID
     *                  <ul>
     *                  <li>
     *                  if <code>-1</code>, recommendations are based on <i>trust</i>;
     *                  </li>
     *                  <li>
     *                  if <code>0</code> recommendations
     *                  are based on <i>trust</i> and <i>user dimensions</i>;
     *                  </li>
     *                  <li>
     *                  if <code>&gt;1</code>, filter by specific dimension ID.
     *                  </li>
     *                  </ul>
     * @param limit     The maximum number of recommendations to retrieve
     * @param options 	The algorithm options for this call
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendations(String uid, String keyword, Integer dimension, int limit,AlgorithmOptions options) throws ApiException;

    
    /**
     * Get recommendations for the supplied user.
     *
     * @param uid The user's ID.
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendations(String uid) throws ApiException;

    /**
     * Retrieve a list of users from a user's trust graph that might like a specific item.
     *
     * @param userId The ID for the user whose trust graph is to be explored.
     * @param itemId The item ID.
     * @return a {@link List} of {@link RecommendedUserBean}s.
     * @throws ApiException in the event of an API error.
     */
    List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId) throws ApiException;

    /**
     * Retrieve a list of users from a user's trust graph that might like a specific item.
     *
     * @param userId The ID for the user whose trust graph is to be explored.
     * @param itemId   The item ID.
     * @param linkType Keyword filter for a specific type of link in the trustgraph
     * @return a {@link List} of {@link RecommendedUserBean}s.
     * @throws ApiException in the event of an API error.
     */
    List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId, String linkType) throws ApiException;

    /**
     * Get recommendations for the supplied user filtered by item type
     *
     * @param uid   The user's ID.
     * @param type  The item type of the recommendations.
     * @param limit The maximum number of recommendations to retrieve
     * @return a {@link List} of recommendation beans
     * @throws ApiException in the event of an API error.
     */
    List<RecommendationBean> getRecommendationsByType(String uid, int type, int limit) throws ApiException;

    /**
     * Retrieve all dimensions.
     *
     * @return a {@link List list} of dimensions.
     * @throws ApiException in the event of an API error.
     */
    List<DimensionBean> getDimensions() throws ApiException;

    /**
     * Retrieve a dimension with the supplied ID.
     *
     * @param dimensionId The ID of the dimension to retrieve
     * @return a dimension bean
     * @throws ApiException in the event of an API error.
     */
    DimensionBean getDimensionById(String dimensionId) throws ApiException;

    /**
     * Retrieve a list of actions for a given user.
     *
     * @param uid   The ID of the user.
     * @param limit The maximum number of actions to retrieve
     * @return a {@link List} of action beans
     * @throws ApiException in the event of an API error.
     */
    List<ActionBean> getUserActions(String uid, int limit) throws ApiException;

    /**
     * Retrieve a list of the most recently defined actions.
     *
     * @return a {@link List} of action beans
     * @throws ApiException in the event of an API error.
     */
    List<ActionBean> getActions() throws ApiException;

    /**
     * Sharing: Retrieve a list of recommended users to share a given item with
     * @param userId The user to get recommendations for
     * @param itemId The item to share
     * @param linkType Filter by links of this type
     * @param keywords Filter by comma separated keywords
     * @param limit The maximum number of recommendations to retrieve
     * @return a {@link List} of recommended users
     * @throws ApiException in the event of an API error.
     */
    List<RecommendedUserBean> getRecommendedUsers(String userId, String itemId, Long linkType, String keywords, Integer limit) throws ApiException;

    /**
     * Retrieve a single action with the supplied ID.
     *
     * @param actionId The ID of the action to retrieve
     * @return an action bean
     * @throws ApiException in the event of an API error.
     */
    ActionBean getActionById(String actionId) throws ApiException;

    /**
     * Retrieve the list of item types
     *
     * @return a {@link List} of itemType beans
     * @throws ApiException in the event of an API error.
     */
    List<ItemTypeBean> getItemTypes() throws ApiException;

    /**
     * Retrieve the list of action types
     *
     * @return a {@link List} of actionType beans
     * @throws ApiException in the event of an API error.
     */
    List<ActionTypeBean> getActionTypes() throws ApiException;

    /**
     * Retrieve the list of users matching the supplied username
     *
     * @param username username to match on
     * @return a {@link List} of user beans
     * @throws ApiException in the event of an API error.
     */
    List<UserBean> getUsersMatchingUsername(String username) throws ApiException;

    List<RecommendationBean> getRankedItems(String uid, List<RecommendationBean> recommendations, Integer limit) throws ApiException;

    List<RecommendationBean> getRankedItems(String uid, List<RecommendationBean> recommendations, Integer limit, AlgorithmOptions algorithmOptions) throws ApiException;

    ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit) throws ApiException;

    ResourceBean getRankedItems(String uid, RecommendationsBean recs, Integer limit, AlgorithmOptions algorithmOptions) throws ApiException;

    /**
     * Add the supplied action.
     *
     * @param actionBean The action to be added (note that the ID field will be ignored)
     *                   This must include at least <i>item</i>, <i>user</i> and <i>type</i>.
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    ActionBean addAction(ActionBean actionBean) throws ApiException;

    /**
     * Update the supplied action
     *
     * @param actionBean The action to be updated
     *                   This must include at least an <i>ID</i>
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    ActionBean updateAction(ActionBean actionBean) throws ApiException;

    /**
     * Add the supplied item
     *
     * @param itemBean The item to be added
     *                 This must include at least an <i>ID</i>
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    ItemBean addItem(ItemBean itemBean) throws ApiException;

    /**
     * Update the supplied item
     *
     * @param itemBean The item to be updated
     *                 This must include at least an <i>ID</i>
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    ItemBean updateItem(ItemBean itemBean) throws ApiException;

    /**
     * Add the supplied user
     *
     * @param userBean The user to be added
     *                 This must include at least an <i>ID</i>
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    UserBean addUser(UserBean userBean) throws ApiException;
    UserBean addUser(UserBean userBean, boolean withSecureConnection) throws ApiException;

    /**
     * Update the supplied user
     *
     * @param userBean The user to be updated
     *                 This must include at least an <i>ID</i>
     * @return The same bean passed in. In future releases this bean will record
     *         additional information provided by the API server.
     * @throws ApiException in the event of an API error.
     */
    UserBean updateUser(UserBean userBean) throws ApiException;
    UserBean updateUser(UserBean userBean, boolean withSecureConnection) throws ApiException;


}
