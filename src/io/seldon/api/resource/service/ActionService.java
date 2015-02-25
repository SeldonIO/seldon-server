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

package io.seldon.api.resource.service;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.logging.ActionLogger;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ActionTypeBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.UserBean;
import io.seldon.api.resource.service.exception.ActionTypeNotFoundException;
import io.seldon.api.service.async.AsyncActionQueue;
import io.seldon.api.service.async.JdoAsyncActionFactory;
import io.seldon.clustering.recommender.ClientClusterTypeService;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.GlobalWeightedMostPopular;
import io.seldon.clustering.recommender.MemoryWeightedClusterCountMap;
import io.seldon.general.Action;
import io.seldon.general.ActionType;
import io.seldon.general.Item;
import io.seldon.general.User;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.ItemsRankingManager;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author claudio
 */

@Service
public class ActionService {
	
	private static Logger logger = Logger.getLogger(ActionService.class.getName());
	
	@Autowired
	private ClientClusterTypeService clusterTypeService;
	

	
	public static ListBean getUserActions(ConsumerBean c, String userId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getUserActionsBeanKey(c.getShort_name(), userId, full, false));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Action> res = Util.getActionPeer(c).getUserActions(UserService.getInternalUserId(c, userId),limit);
			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUserActionsBeanKey(c.getShort_name(), userId, full, false),bean,Constants.CACHING_TIME);
		}
		return bean;
			
	}

	public static ListBean getItemActions(ConsumerBean c, String itemId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getItemActionsBeanKey(c.getShort_name(), itemId, full, false));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Action> res = Util.getActionPeer(c).getItemActions(ItemService.getInternalItemId(c, itemId), limit);
			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemActionsBeanKey(c.getShort_name(), itemId, full, false),bean,Constants.CACHING_TIME);
		}
		return bean;
	} 
	
	public static ListBean getActions(ConsumerBean c, String userId, String itemId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getUserItemActionBeanKey(c.getShort_name(), userId, itemId, full, false));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Action> res = Util.getActionPeer(c).getUserItemActions(ItemService.getInternalItemId(c, itemId), UserService.getInternalUserId(c, userId), limit);
			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUserItemActionBeanKey(c.getShort_name(), userId, itemId, full, false),bean,Constants.CACHING_TIME);
		}
		return bean;
	}
	
	
	public static ListBean getActions(ConsumerBean c, long userId, long itemId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getInternalUserItemActionBeanKey(c.getShort_name(), userId, itemId, full, false));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Action> res = Util.getActionPeer(c).getUserItemActions(itemId, userId, limit);
			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getInternalUserItemActionBeanKey(c.getShort_name(), userId, itemId, full, false),bean,Constants.CACHING_TIME);
		}
		return bean;
	}

// TODO: setup another memcache key for a specific action type?
//	public static ListBean getActions(ConsumerBean c, String userId, String itemId, int type, int limit, boolean full) throws APIException {
//		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getUserItemActionBeanKey(c.getShort_name(), userId, itemId, full));
//		bean = Util.getLimitedBean(bean, limit);
//		if(bean == null) {
//			bean = new ListBean();
//			Collection<Action> res = Util.getActionPeer(c).getUserItemActions(ItemService.getInternalItemId(c, itemId), UserService.getInternalUserId(c, userId), limit);
//			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
//			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUserItemActionBeanKey(c.getShort_name(), userId, itemId, full),bean,Constants.CACHING_TIME);
//		}
//		return bean;
//	}
	
	public static ActionBean getAction(ConsumerBean c,long actionId,boolean full) throws APIException  {
		ActionBean bean =  (ActionBean)MemCachePeer.get(MemCacheKeys.getActionBeanKey(c.getShort_name(), actionId, full, false));
		if(bean==null) {
			Action a = Util.getActionPeer(c).getAction(actionId);
			bean = new ActionBean(a,c,full);
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getActionBeanKey(c.getShort_name(), actionId, full, false),bean,Constants.CACHING_TIME);
		}
		return bean;
	}

	public static ResourceBean getRecentActions(ConsumerBean c,int limit,boolean full) throws APIException {
			ListBean bean = new ListBean();
			Collection<Action> res = Util.getActionPeer(c).getRecentActions(limit);
			for(Action a : res) { bean.addBean(new ActionBean(a,c,full)); }
		return bean;
	}
	
	public void addAction(ConsumerBean c,ActionBean bean) {
		
		JdoAsyncActionFactory asyncFactory = JdoAsyncActionFactory.get();
		AsyncActionQueue q = null;
		if (asyncFactory != null)
			q = asyncFactory.get(c.getShort_name());
		boolean doAsyncAction = q != null;
		
		
		//check if user and item exist
		//if not it adds them to the db
		Long userId = null;
		Long itemId = null;
		
		try 
		{ 
			itemId = ItemService.getInternalItemId(c, bean.getItem()); 
		}
		catch(APIException e) 
		{
			if (!doAsyncAction || !JdoAsyncActionFactory.isAsyncItemWrites())
			{
				if(e.getError_id()==APIException.ITEM_NOT_FOUND) 
				{
					//TODO - addItem can throw an exception if item is now already created - change?
					try
					{
						Item item = ItemService.addItem(c, new ItemBean(bean.getItem()));
						itemId = item.getItemId();
					}
					catch (APIException e2)
					{
						if(e2.getError_id()==APIException.ITEM_DUPLICATED)
							itemId = ItemService.getInternalItemId(c, bean.getItem()); 
						else
							throw e2;
					}
				}
				else
					throw e;
			}
			else
				itemId=0L;
		};
		
		try 
		{ 
			userId = UserService.getInternalUserId(c, bean.getUser()); 
		}
		catch(APIException e) 
		{
			if (!doAsyncAction || !JdoAsyncActionFactory.isAsyncUserWrites())
			{
				if(e.getError_id()==APIException.USER_NOT_FOUND) 
				{
					//TODO - addUser can throw an exception if user is now already created - change?
					try
					{
						User user = UserService.addUser(c, new UserBean(bean.getUser()));
						userId = user.getUserId();
					}
					catch(APIException e2) 
					{
						if(e2.getError_id()==APIException.USER_DUPLICATED)
							userId = UserService.getInternalUserId(c, bean.getUser()); 
						else
							throw e2;
					}
				}
				else
					throw e;
			}
			else
				userId=0L;
				
		}; 

		if (userId != null && itemId != null)
		{
			Action a = bean.createAction(c,userId,itemId);
			if (TestingUtils.get().getTesting())
				TestingUtils.get().setLastActionTime(a.getDate());
			logger.debug("Action created with Async "+doAsyncAction+" for client "+c.getShort_name()+" userId:"+userId+" itemId:"+itemId+" clientUserId:"+a.getClientUserId()+" clientItemId:"+a.getClientItemId());
			
			ActionLogger.log(c.getShort_name(), userId, itemId, a.getType(), a.getValue(), a.getClientUserId(), a.getClientItemId(), bean.getRecTag());
			
			if(doAsyncAction)
			{
				q.put(a);
			}
			else
			{
				Util.getActionPeer(c).addAction(a);
			}
			
			//Add to count recommender if applicable to client and action cache
			if (userId > 0 && itemId > 0)
			{
				
				CountRecommender cRec = Util.getCountRecommenderUtils(c).getCountRecommender(c.getShort_name());
				if (cRec != null)
				{
					boolean addCount = true;
					if (a.getType() != null)
						addCount = clusterTypeService.okToClusterCount(c.getShort_name(), a.getType());
					if (addCount)
					{
						if (bean.getReferrer() != null)
							cRec.setReferrer(bean.getReferrer());
						if (a.getDate() != null)
							cRec.addCount(userId, itemId,a.getDate().getTime()/1000);
						else
							cRec.addCount(userId, itemId);
					}
				}
				

				if (ActionHistoryCache.isActive(c.getShort_name()))
				{
					ActionHistoryCache ah = new ActionHistoryCache(c.getShort_name());
					ah.addAction(userId, itemId);
				}
				
				
			}
			
			//add item to the global lists
			if(itemId > 0 ) {
				
				ItemsRankingManager.getInstance().Hit(c.getShort_name(), itemId);
				
				if (GlobalWeightedMostPopular.isActive())
				{
					MemoryWeightedClusterCountMap m =  GlobalWeightedMostPopular.get(c.getShort_name());
					if (a.getDate() != null)
						m.incrementCount(itemId, 1, a.getDate().getTime()/1000);
					else
						m.incrementCount(itemId, 1, System.currentTimeMillis()/1000);
				}
			}
		}
		else {
//			logger.error("UserId or ItemId is null when adding action "+bean);
            final String message = "UserId or ItemId is null when adding action " + bean;
            logger.error(message, new Exception(message));
        }
	}
	
	public static void addAction(ConsumerBean c,Action action) {
		//check if user and item exist
		//if not it adds them to the db
		/* try { ItemService.getInternalItemId(c, bean.getItem()); }
		catch(APIException e) {if(e.getError_id()==APIException.ITEM_NOT_FOUND) ItemService.addItem(c, new ItemBean(bean.getItem()));};
		try { UserService.getInternalUserId(c, bean.getUser()); }
		catch(APIException e) {if(e.getError_id()==APIException.USER_NOT_FOUND) UserService.addUser(c, new UserBean(bean.getUser()));}; 
		Action a = bean.createAction(c); */
		Util.getActionPeer(c).addAction(action);
		//global lists
		if(action.getItemId() > 0) {
			ItemsRankingManager.getInstance().Hit(c.getShort_name(),action.getItemId());
		}
	}
	
	public static ActionType getActionType(ConsumerBean c, String name) throws ActionTypeNotFoundException {
        String actionTypeKey = MemCacheKeys.getActionTypeByName(c.getShort_name(), name);
        ActionType at = (ActionType) MemCachePeer.get(actionTypeKey);
		if(at==null) {
			at = Util.getActionPeer(c).getActionType(name);
            if ( at == null ) {
                throw new ActionTypeNotFoundException(c.getShort_name(), name);
            }
			MemCachePeer.put(actionTypeKey, at,Constants.CACHING_TIME);
		}
		return at;
	}
	
	public static ActionType getActionType(ConsumerBean c, int typeId) {
		ActionType at = (ActionType) MemCachePeer.get(MemCacheKeys.getActionTypeById(c.getShort_name(), typeId));
		if(at==null) {
			at = Util.getActionPeer(c).getActionType(typeId);
			MemCachePeer.put(MemCacheKeys.getActionTypeById(c.getShort_name(), typeId), at, Constants.CACHING_TIME);
		}
		return at;
	}

	public static ResourceBean getActionTypes(ConsumerBean c) {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getActionTypes(c.getShort_name()));
		if(bean == null) {
			bean = new ListBean();
			Collection<ActionType> types = Util.getActionPeer(c).getActionTypes();
			for(ActionType t : types) { bean.addBean(new ActionTypeBean(t)); }
			MemCachePeer.put(MemCacheKeys.getActionTypes(c.getShort_name()), bean,Constants.CACHING_TIME);
		}
			return bean;
	}
	
	
}
