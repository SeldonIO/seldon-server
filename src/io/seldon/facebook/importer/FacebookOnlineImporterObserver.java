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

package io.seldon.facebook.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;

import org.apache.log4j.Logger;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.UserService;
import io.seldon.clustering.recommender.CreateClustersUtils;
import io.seldon.clustering.recommender.MemoryUserClusterStore;
import io.seldon.clustering.recommender.TransientUserClusterStore;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.clustering.recommender.jdo.JdoUserClusterStore;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.facebook.FacebookUserGraph;
import io.seldon.facebook.Like;
import io.seldon.general.User;

/**
 * Created by: marc on 17/01/2012 at 15:26
 */
public class FacebookOnlineImporterObserver extends FacebookImporterObserver {
    private static final Logger logger = Logger.getLogger(FacebookOnlineImporterObserver.class);

    private String queuePrefix;

    public FacebookOnlineImporterObserver(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    @Override
    protected void processGraph(UserBean userBean, ConsumerBean consumerBean, String facebookId, FacebookUserGraph userGraph) {
        CreateClustersUtils utils = createUtils(consumerBean.getShort_name());

        final Set<String> likeIds = new HashSet<String>();
        final Set<String> likeNames = new HashSet<String>();

        final Map<String, Like> likes = userGraph.getLikes();
        final List<String> categories = new ArrayList<String>();
        // like contents
        if (likes != null) {
            for (Map.Entry<String, Like> entry : likes.entrySet()) {
                final String likeId = entry.getKey();
                final Like like = entry.getValue();
                final String likeName = like.getName();
                likeNames.add(likeName);
                likeIds.add(Constants.FACEBOOK_ID_PREFIX + likeId);
                categories.add(like.getCategory());
            }
        }

//        String queueName;
//        if (JDOFactory.isDefaultClient(consumerBean.getShort_name()))
//        	queueName = queuePrefix + Constants.DEFAULT_CLIENT;
//        else
//        	queueName = queuePrefix + consumerBean.getShort_name();
//        String message = userBean.getId();
//        try {
//            logger.info("*** Creating queue...");
//            SimpleAWSQueue awsQueue = new SimpleAWSQueue(queueName);
//            logger.info("*** Adding user to queue: " + queueName + " >> " + message);
//            //offlineProcessingQueue.sendMessage(queueName, message, null);
//            awsQueue.sendMessage(message);
//
//        } catch (AWSSQSProvider.AWSQueueNotFoundException e) {
//            logger.warn("Unable to connect to AWS queue: " + queueName + " for ID ", e);
//        } catch (Exception e) {
//            logger.warn("Unable to queue offline processing trigger to " + queueName + " for ID " + message, e);
//        }

        
         
        Long internalUserId;
        try {
            internalUserId = retrieveInternalUserId(userBean, consumerBean);
            logger.info("User ID: " + internalUserId+" will search for clusters");
            utils.createClustersFromCategoriesOrTags(internalUserId, likeNames, categories);
        } catch (APIException e) {
            logger.error("Unable to add/retrieve user so can't create clusters : " + userBean, e);
            return;
        }
        
        
    }

    private void persistLike(ConsumerBean consumerBean, Like like) {
        ItemBean bean = itemFromLike(like);
        try {
            ItemService.updateItem(consumerBean, bean);
        } catch (APIException e) {
            logger.warn("Problem adding like " + like + ".", e);
        }
    }

    /**
     * Ideally, this should be generalised into a factory/adapter-style pattern.
     *
     * @param like the {@link Like} instance to be persisted
     * @return an {@link ItemBean} instance
     */
    private ItemBean itemFromLike(Like like) {
        ItemBean item = new ItemBean();
        item.setName(like.getName());
        item.setId(Constants.FACEBOOK_ID_PREFIX + like.getId());
        Map<String, String> attributesMap = new HashMap<String, String>();
        attributesMap.put("category", like.getCategory());
        item.setAttributesName(attributesMap);
        // TODO
        item.setType(Constants.DEFAULT_ITEM_TYPE);
        return item;
    }

    /**
     * Try to retrieve the internal user ID for the supplied user, creating a User if necessary.
     *
     * @param userBean     ...
     * @param consumerBean ...
     * @return the internal user id
     */
    private Long retrieveInternalUserId(UserBean userBean, ConsumerBean consumerBean) {
        Long internalUserId;
        try {
            internalUserId = UserService.getInternalUserId(consumerBean, userBean.getId());
        } catch (APIException e) {
            logger.info("User not found -- creating...");
            try {
                User user;
                user = UserService.addUser(consumerBean, userBean);
                internalUserId = user.getUserId();
            } catch (Exception e1) {
                // Claudio's suggestion: perhaps the user has been added in the meantime...
                internalUserId = UserService.getInternalUserId(consumerBean, userBean.getId());
            }
        }
        return internalUserId;
    }

    private CreateClustersUtils createUtils(String consumerName) {
        PersistenceManager persistenceManager = JDOFactory.getPersistenceManager(consumerName);
        MemoryUserClusterStore userClusters = JdoMemoryUserClusterFactory.get().get(consumerName);
        final TransientUserClusterStore userClusterStore = new JdoUserClusterStore(persistenceManager);
        return new CreateClustersUtils(consumerName, userClusterStore, userClusters);
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }


}
