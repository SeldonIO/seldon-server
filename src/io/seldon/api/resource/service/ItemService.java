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

import java.util.*;


import io.seldon.api.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.caching.ClientIdCacheStore;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.DimensionBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ItemTypeBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.general.Dimension;
import io.seldon.general.Item;
import io.seldon.general.ItemAttr;
import io.seldon.general.ItemType;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.ItemsRankingManager;
import io.seldon.trust.impl.RummbleLabsAPI;
import io.seldon.trust.impl.SearchResult;



/**
 * @author claudio
 */

@Service
public class ItemService {

    private static Logger logger = Logger.getLogger(ItemService.class.getName());
    public static final int ITEM_NAME_LENGTH = 255;
    private static final int ITEMS_CACHING_TIME_SECS = 300;
    
    public static ItemBean getItem(final ConsumerBean c, final String iid, final boolean full) throws APIException
    {
    	String memKey = MemCacheKeys.getItemBeanKey(c.getShort_name(), iid,full);
    	ItemBean res =  (ItemBean) MemCachePeer.get(memKey);
    	ItemBean newRes = null;
    	try
    	{
    		newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, res, new UpdateRetriever<ItemBean>() {
			@Override
			public ItemBean retrieve() throws Exception {
				Item i = Util.getItemPeer(c).getItem(iid);
				return new ItemBean(i,full,c);
			}
    		},Constants.CACHING_TIME);
    	}
    	catch (Exception e)
    	{
			logger.warn("Error when retrieving item bean in dogpile handler ", e);
		}
		if (newRes != null)
		{
			MemCachePeer.put(memKey, newRes,Constants.CACHING_TIME);
			res = newRes;
		}
		if(newRes==null && res == null)
		{
			throw new APIException(APIException.ITEM_NOT_FOUND);
		}
		return res;
    }
    
    public static ItemBean getItemOld(ConsumerBean c, String iid, boolean full) throws APIException {
		ItemBean bean = (ItemBean)MemCachePeer.get(MemCacheKeys.getItemBeanKey(c.getShort_name(), iid,full));
		if(bean == null) {
			Item i = Util.getItemPeer(c).getItem(iid);
            if ( i == null ) {
                // TODO We should throw a checked exception (using APIException for now; minimal surprises).
                throw new APIException(APIException.ITEM_NOT_FOUND);
            }
			bean = new ItemBean(i,full,c);
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemBeanKey(c.getShort_name(), iid,full),bean,Constants.CACHING_TIME);
		}
		return bean;
	}
	
	public static ListBean getItems(ConsumerBean c, int limit, boolean full, String sort,int dimension) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getItemsBeanKey(c.getShort_name(),full,sort,dimension));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			bean.setRequested(limit);
			Collection<Item> res = null;
			if(sort == null || sort.length() == 0 || sort.toLowerCase().equals(Constants.SORT_ID)) { res = Util.getItemPeer(c).getItems(limit,dimension,c); }
			else if(sort.toLowerCase().equals(Constants.SORT_NAME)) { res  = Util.getItemPeer(c).getAlphabeticItems(limit,dimension,c); }
			else if(sort.toLowerCase().equals(Constants.SORT_LAST_ACTION)) { res = Util.getItemPeer(c).getRecentItems(limit,dimension,c); }
			else if(sort.toLowerCase().equals(Constants.SORT_DATE)) { 
				List<Long> list = ItemsRankingManager.getInstance().getItemsSortedByDate(c.getShort_name(), limit);
				for(Long item : list) { bean.addBean(ItemService.getItem(c, getClientItemId(c,item), full)); }
			}
			else if(sort.toLowerCase().equals(Constants.SORT_POPULARITY)) { 
				List<Long> list = ItemsRankingManager.getInstance().getItemsSortedByPopularity(c.getShort_name(), limit);
				for(Long item : list) { bean.addBean(ItemService.getItem(c, getClientItemId(c,item), full)); }
			}
			if(res!=null) { for(Item i : res) { bean.addBean(new ItemBean(i,full,c)); }}
			if(bean != null) bean.setSize(bean.getList().size());
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemsBeanKey(c.getShort_name(), full, sort,dimension),bean,ITEMS_CACHING_TIME_SECS);
		}
		return bean;
	} 
	
	public static ListBean getItemsByKeyword(ConsumerBean c, List<String> keywords, int limit,boolean full,int dimension) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getItemsBeanKey(c.getShort_name(),keywords.toString(), full));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
            CFAlgorithm cfAlgorithm;
            try {
                cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c);
            } catch (CloneNotSupportedException e) {
                throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
            }
			RummbleLabsAPI tp = Util.getLabsAPI(cfAlgorithm);
            int counter = 1;
            for(String k : keywords) {
				List<SearchResult> res = tp.getAnalysis(cfAlgorithm).searchContent(k,null,ItemService.getDimension(c, dimension),limit,cfAlgorithm);
				for(SearchResult r : res) {
					Item i = null;
					ItemBean ib = null;
					if(full) { i = Util.getItemPeer(c).getItem(r.getId()); }
					if(i!=null) { ib = new ItemBean(i,full,c); }
					else {	ib = new ItemBean(c,r.getId());	}
					bean.addBean(ib);
					if(counter++>limit) { return bean; }
				}
				bean.setRequested(limit);
				bean.setSize(bean.getSize()+res.size());
			}
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemsBeanKey(c.getShort_name(),keywords.toString(),full),bean,ITEMS_CACHING_TIME_SECS);	
		}
		return bean;
	}
	
	public static ListBean getItemsByName(ConsumerBean c, int limit, boolean full, String name, int dimension) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getItemsBeanKeyByName(c.getShort_name(), full, name,dimension));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Item> res = Util.getItemPeer(c).getItemsByName(name,limit,dimension,c);
			for(Item i : res) { bean.addBean(new ItemBean(i,full,c)); }
			bean.setRequested(limit);
			bean.setSize(res.size());
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemsBeanKeyByName(c.getShort_name(), full, name,dimension),bean,ITEMS_CACHING_TIME_SECS);
		}
		return bean;
	}
	
	
	public static DimensionBean getDimension(ConsumerBean c,int dimension) throws APIException {
		if(dimension == Constants.DEFAULT_DIMENSION) return null;
		DimensionBean bean = (DimensionBean)MemCachePeer.get(MemCacheKeys.getDimensionBeanKey(c.getShort_name(), dimension));
		if(bean == null) {
			bean = new DimensionBean();
			bean.setDimId(dimension);
			int itemType = Util.getItemPeer(c).getDimensionItemType(dimension);
			Integer[] attr = Util.getItemPeer(c).getAttributes(dimension);
			String[] attrName = Util.getItemPeer(c).getAttributesNames(dimension);
			bean.setItemType(itemType);
			bean.setAttr(attr[0]);
			bean.setVal(attr[1]);
            // TODO address this properly:
            if ( attrName == null ) {
                bean.setAttrName("<all>");
                bean.setValName("<all>");
            } else {
                bean.setAttrName(attrName[0]);
                bean.setValName(attrName[1]);
            }
			MemCachePeer.put(MemCacheKeys.getDimensionBeanKey(c.getShort_name(), dimension),bean,Constants.CACHING_TIME);
		}
		return bean;
	}
	
	public static DimensionBean getDimension(ConsumerBean c,int attr,int val) throws APIException {
		DimensionBean bean = (DimensionBean)MemCachePeer.get(MemCacheKeys.getDimensionBeanKey(c.getShort_name(),attr,val));
		if(bean == null) {
			bean = new DimensionBean();
			bean.setAttr(attr);
			bean.setVal(val);
			int dimension = Util.getItemPeer(c).getDimension(attr,val);
			int itemType = Util.getItemPeer(c).getDimensionItemType(dimension);
			String[] attrName = Util.getItemPeer(c).getAttributesNames(dimension);
			bean.setDimId(dimension);
			bean.setItemType(itemType);
			bean.setAttrName(attrName[0]);
			bean.setValName(attrName[1]);
			MemCachePeer.put(MemCacheKeys.getDimensionBeanKey(c.getShort_name(),attr,val),bean,Constants.CACHING_TIME);
		}
		return bean;
	}
	
	public static DimensionBean getDimension(ConsumerBean c,String attrName,String valName) throws APIException {
		DimensionBean bean = (DimensionBean)MemCachePeer.get(MemCacheKeys.getDimensionBeanKey(c.getShort_name(),attrName,valName));
		if(bean == null) {
			int dimension = Util.getItemPeer(c).getDimension(attrName,valName);
			if(dimension != Constants.DEFAULT_DIMENSION) {
				bean = new DimensionBean();
				bean.setAttrName(attrName);
				bean.setValName(valName);
				int itemType = Util.getItemPeer(c).getDimensionItemType(dimension);
				Integer[] attr = Util.getItemPeer(c).getAttributes(dimension);
				bean.setDimId(dimension);
				bean.setItemType(itemType);
				bean.setAttr(attr[0]);
				bean.setVal(attr[1]);
				MemCachePeer.put(MemCacheKeys.getDimensionBeanKey(c.getShort_name(),attrName,valName),bean,Constants.CACHING_TIME);
			}
			else return null;
		}
		return bean;
	}
	
	public static ListBean getDimensions(ConsumerBean c) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getDimensionsBeanKey(c.getShort_name()));
		if(bean == null) {
			bean = new ListBean();
			Collection<Dimension> dims = Util.getItemPeer(c).getDimensions();
			for(Dimension d : dims) { bean.addBean(new DimensionBean(d)); }
			MemCachePeer.put(MemCacheKeys.getDimensionsBeanKey(c.getShort_name()),bean,Constants.CACHING_TIME);
		}
		return bean;
	}

	public static Integer getDimensionbyItemType(ConsumerBean c,int itemType) throws APIException {
		Integer res = (Integer)MemCachePeer.get(MemCacheKeys.getDimensionBeanByItemTypeKey(c.getShort_name(),itemType));
		if(res == null) {
			res = Util.getItemPeer(c).getDimensionByItemType(itemType);
			if (res != null)
				MemCachePeer.put(MemCacheKeys.getDimensionBeanByItemTypeKey(c.getShort_name(),itemType),res,Constants.CACHING_TIME);
			else
				logger.warn("Can't get dimension for item type "+itemType);
		}
		return res;
	}
	
	
	public static Long getInternalItemId(ConsumerBean c, String id) throws APIException {
		Long res = null;
		res = ClientIdCacheStore.get().getInternalItemId(c.getShort_name(), id);
		if (res == null)
			res = (Long)MemCachePeer.get(MemCacheKeys.getItemInternalId(c.getShort_name(), id));
		if(res==null) {
			Item i = Util.getItemPeer(c).getItem(id);
			if(i!=null) {
				res = i.getItemId();
				ClientIdCacheStore.get().putItemId(c.getShort_name(), id, res);
                cacheInternalItemId(c, id, res);
            }
			else {
				logger.warn("getInternalItemId(" + id + "): ITEM NOT FOUND");
				throw new APIException(APIException.ITEM_NOT_FOUND);
			}
		}
		return res;
	}

    /**
     * Cache an item's internal ID keyed by client ID
     * @param consumerBean -
     * @param clientId the client item ID (cache key)
     * @param internalId the internal item ID (cache value)
     */
    public static void cacheInternalItemId(ConsumerBean consumerBean, String clientId, Long internalId) {
        final String clientIdKey = MemCacheKeys.getItemInternalId(consumerBean.getShort_name(), clientId);
        MemCachePeer.put(clientIdKey, internalId,Constants.CACHING_TIME);
    }

    public static String getClientItemId(ConsumerBean c, Long id) throws APIException {
		if(id == null) { throw new APIException(APIException.ITEM_NOT_FOUND); }
		String res = null;
		res = ClientIdCacheStore.get().getExternalItemId(c.getShort_name(), id);
		if (res == null)
			res = (String)MemCachePeer.get(MemCacheKeys.getItemClientId(c.getShort_name(), id));
		if(res==null) {
			Item i = Util.getItemPeer(c).getItem(id);
			if(i!=null) {
				res = i.getClientItemId();
				ClientIdCacheStore.get().putItemId(c.getShort_name(), res, id);
                cacheClientItemId(c, id, res);
            }
			else {
				logger.warn("getClientItemId(" + id + "): ITEM NOT FOUND");
				throw new APIException(APIException.ITEM_NOT_FOUND);
			}
		}
		return res;
	}

    /**
     * Cache an item's client ID keyed by item ID
     * @param consumerBean -
     * @param internalId the internal item ID (cache key)
     * @param clientId the client item ID (cache value)
     */
    public static void cacheClientItemId(ConsumerBean consumerBean, Long internalId, String clientId) {
        final String internalItemKey = MemCacheKeys.getItemClientId(consumerBean.getShort_name(), internalId);
        MemCachePeer.put(internalItemKey, clientId,Constants.CACHING_TIME);
    }

    /**
     * Bidirectionally cache the supplied item
     * (see {@link ItemService#cacheClientItemId(io.seldon.api.resource.ConsumerBean, Long, String)}
     * and {@link ItemService#cacheInternalItemId(io.seldon.api.resource.ConsumerBean, String, Long)}).
     * @param consumerBean -
     * @param internalId user ID
     * @param clientId client ID
     */
    public static void cacheItem(ConsumerBean consumerBean, Long internalId, String clientId) {
        cacheClientItemId(consumerBean, internalId, clientId);
        cacheInternalItemId(consumerBean, clientId, internalId);
    }

    public static String getAttrType(ConsumerBean c, int itemType,String attrName) {
		String res = (String)MemCachePeer.get(MemCacheKeys.getItemAttrType(c.getShort_name(), itemType, attrName));
		if(res==null) {
			ItemAttr a = Util.getItemPeer(c).getItemAttr(itemType,attrName);
            if ( a != null ) {
			    res = a.getType();
			    MemCachePeer.put(MemCacheKeys.getItemAttrType(c.getShort_name(), itemType, attrName), res,Constants.CACHING_TIME);
            }
		}
		return res;
	}

    private static void truncateItemName(ItemBean itemBean, int newLength) {
        final String name = itemBean.getName();
        final String truncatedName = StringUtils.left(name, newLength);
        logger.info("Truncating itemBean name '" + name + "' to '" + truncatedName + "'");
        itemBean.setName(truncatedName);
    }
	
	public static Item addItem(ConsumerBean c,ItemBean bean) {
		//check if the item is already in the system
		try { 
			getInternalItemId(c, bean.getId());
			throw new APIException(APIException.ITEM_DUPLICATED);
		}
		catch(APIException e) {
			if(e.getError_id() != APIException.ITEM_NOT_FOUND) {
				throw e;
			}
		}
        truncateItemName(bean, ITEM_NAME_LENGTH);
		Item i = bean.createItem(c);
        // double check the type is valid,
        ItemType type = ItemService.getItemType(c, i.getType());
        if ( type == null ) {
            throw new APIException(APIException.ITEM_TYPE_NOT_FOUND);
        }
        i = Util.getItemPeer(c).addItem(i,c);
        long itemId = i.getItemId();
        if(bean.getAttributesName() != null && bean.getAttributesName().size()>0) {
            Util.getItemPeer(c).addItemAttributeNames(itemId, type.getTypeId(), bean.getAttributesName(), c);
        }
        else if(bean.getAttributes() != null && bean.getAttributes().size()>0) {
            Util.getItemPeer(c).addItemAttribute(itemId, type.getTypeId(), bean.getAttributes(),c);
        }
        //add item to the global lists
        final String consumerName = c.getShort_name();
        ItemsRankingManager.getInstance().addItem(consumerName,i.getItemId(), new Date());
        return i;
    }

    public static void updateItem(ConsumerBean c,ItemBean bean) {
        Long itemId = null;
        truncateItemName(bean, ITEM_NAME_LENGTH);
        //check if the item is already in the system
        try {
			itemId = getInternalItemId(c, bean.getId());
			Item i = bean.createItem(c);
			i.setItemId(itemId);
//			i = Util.getItemPeer(c).addItem(i,c);
			ItemType type = ItemService.getItemType(c, i.getType());
	        if ( type == null ) {
	            throw new APIException(APIException.ITEM_TYPE_NOT_FOUND);
	        }
			i = Util.getItemPeer(c).saveOrUpdate(i,c);
            // TODO:
			if(bean.getAttributesName() != null && bean.getAttributesName().size()>0) {
				Util.getItemPeer(c).addItemAttributeNames(itemId, bean.getType(), bean.getAttributesName(),c);
			}
			else if(bean.getAttributes() != null && bean.getAttributes().size()>0) {
				Util.getItemPeer(c).addItemAttribute(itemId, bean.getType(), bean.getAttributes(),c);
			}
			//invalidate memcache entry (both full and short version)
			MemCachePeer.delete(MemCacheKeys.getItemBeanKey(c.getShort_name(), bean.getId(),true));
			MemCachePeer.delete(MemCacheKeys.getItemBeanKey(c.getShort_name(), bean.getId(),false));
			MemCachePeer.delete(MemCacheKeys.getItemDimensions(c.getShort_name(), itemId));
		}
		catch(APIException e) {
			if(e.getError_id() == APIException.ITEM_NOT_FOUND) {
                try {
                    addItem(c,bean);
                } catch (APIException additionException) {
                    if ( additionException.getError_id() == APIException.ITEM_DUPLICATED) {
                        throw new APIException(APIException.CONCURRENT_ITEM_UPDATE);
                    } else {
                        throw additionException;
                    }
                }
            }
			else {
				throw e;
			}
		}
	}
	
	public static Collection<Integer> getItemDimensions(ConsumerBean c, long itemId) {
		Collection<Integer> res = (Collection<Integer>)MemCachePeer.get(MemCacheKeys.getItemDimensions(c.getShort_name(), itemId));
		if(res == null) {
			res = Util.getItemPeer(c).getItemDimensions(itemId);
			MemCachePeer.put(MemCacheKeys.getItemDimensions(c.getShort_name(), itemId), res,Constants.CACHING_TIME);
		}
		return res;
	}
	
	public static Integer getItemCluster(ConsumerBean c, long itemId) {
		Integer res = (Integer)MemCachePeer.get(MemCacheKeys.getItemCluster(c.getShort_name(), itemId));
		if(res == null) { 
			res = Util.getItemPeer(c).getItemCluster(itemId);
			MemCachePeer.put(MemCacheKeys.getItemCluster(c.getShort_name(), itemId), res,ITEMS_CACHING_TIME_SECS);
		}
		return res;
	}
	
	
	public static ItemType getItemType(ConsumerBean c, String name) {
        ItemType res = (ItemType) MemCachePeer.get(MemCacheKeys.getItemTypeByName(c.getShort_name(), name));
		if(res==null) {
			ItemType t = Util.getItemPeer(c).getItemType(name);
            if ( t == null ) {
                throw new APIException(APIException.ITEM_TYPE_NOT_FOUND);
            }
            MemCachePeer.put(MemCacheKeys.getItemTypeByName(c.getShort_name(), name), t,ITEMS_CACHING_TIME_SECS);
            res = t;
		}
		return res;
	}
	
	public static ItemType getItemType(ConsumerBean c, int typeId) {
		ItemType res = (ItemType) MemCachePeer.get(MemCacheKeys.getItemTypeById(c.getShort_name(), typeId));
		if(res==null) {
			ItemType t = Util.getItemPeer(c).getItemType(typeId);
			MemCachePeer.put(MemCacheKeys.getItemTypeById(c.getShort_name(), typeId), t,ITEMS_CACHING_TIME_SECS);
            res = t;
		}
		return res;
	}
	
	public static ResourceBean getItemTypes(ConsumerBean c) {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getItemTypes(c.getShort_name()));
		if(bean == null) {
			bean = new ListBean();
			Collection<ItemType> types = Util.getItemPeer(c).getItemTypes();
			for(ItemType t : types) { bean.addBean(new ItemTypeBean(t)); }
			MemCachePeer.put(MemCacheKeys.getItemTypes(c.getShort_name()), bean,ITEMS_CACHING_TIME_SECS);
		}
			return bean;
	}

	public static List<String> getItemSemanticAttributes(ConsumerBean c, long itemId) {
		List<String> res = (List<String>)MemCachePeer.get(MemCacheKeys.getItemSemanticAttributes(c.getShort_name(),itemId));
		if(res==null) {
			res = Util.getItemPeer(c).getItemSemanticAttributes(itemId);
			MemCachePeer.put(MemCacheKeys.getItemSemanticAttributes(c.getShort_name(),itemId), res,Constants.CACHING_TIME);
		}
		return res;
	}

	public static ItemBean filter(ItemBean itemBean, List<String> attributeList) {
		if(attributeList != null && attributeList.size()>0) {
			Map<String,String> oldAttributes = itemBean.getAttributesName();
			Map<String,String> newAttributes = new HashMap<>();
			for(String attribute : attributeList) {
				newAttributes.put(attribute, oldAttributes.get(attribute));
			}
			itemBean.setAttributesName(newAttributes);
		}
		return itemBean;
	}
		
}
