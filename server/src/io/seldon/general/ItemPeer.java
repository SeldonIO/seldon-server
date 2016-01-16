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

package io.seldon.general;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.general.jdo.SqlItemPeer;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ItemPeer {
	
	public abstract Item getItem(long itemId);
	public abstract Item getItem(String itemId);
	
	public abstract Collection<Item> getAlphabeticItems(int limit,int dimension,ConsumerBean cb);
	public abstract Collection<Item> getItems(int limit,int dimension,ConsumerBean cb);
	public abstract Collection<Item> getItems(int skip,int limit);
	public abstract Collection<Item> getRecentItems(int limit,int dimension,ConsumerBean cb);
	public abstract Integer[] getAttributes(int itemId);
	public abstract String[] getAttributesNames(int itemId);
	public abstract Collection<String> getItemAttributesNameByAttrName(long itemId,String attrName);
	public abstract int getDimension(int attr,int val);
	public abstract int getDimension(String attrName,String valName);

	public abstract Map<Integer,Integer> getItemAttributes(long id);
	public abstract Map<String,String> getItemAttributesName(long id);
	public abstract double getItemAvgRating(long itemId, int dimension);
	public abstract Collection<Dimension> getDimensions();
	public abstract Collection<Integer> getItemDimensions(long id);

    public abstract Item saveOrUpdate(final Item item, ConsumerBean consumerBean);

	//TODO if adding an item with same client_item_id replace the previous element in the db instead of duplicating
	public abstract Item addItem(Item i, ConsumerBean c);
	public abstract boolean addItemAttribute(long itemId, int itemType, Map<Integer,Integer> attributes, ConsumerBean c);
	public abstract boolean addItemAttributeNames(long itemId, int itemType, Map<String,String> attributes,ConsumerBean c);
	public abstract Collection<Item> getItemsByName(String name, int limit, int type,ConsumerBean cb);
	public abstract ItemAttr getItemAttr(int itemType, String attrName);
	public abstract ItemType getItemType(String name);
	public abstract ItemType getItemType(int typeId);
	public abstract Collection<ItemDemographic> getItemDemographics(long itemId);
	
	public abstract Collection<Item> getItemsFromUserActions(long user_id,String action_type,int limit);
	public abstract Collection<ItemType> getItemTypes();
	public abstract Collection<String> getItemSemanticAttributes(long itemId);
	public abstract long getMinItemId(Date after,Integer type,ConsumerBean c);
	public abstract List<Long> getRecentItemIds(Set<Integer> dimensions,int limit,ConsumerBean c);
	public abstract List<Long> getRecentItemIdsTwoDimensions(Set<Integer> dimensions,int dimension2,int limit,ConsumerBean c);
	public abstract Map<Long,List<String>> getRecentItemTags(Set<Long> ids,int attrId,String table);
	public abstract List<Long> getRecentItemIdsWithTags(int tagAttrId,Set<String> tags, int limit);
	
	public abstract Integer getDimensionForAttrName(long itemId,String name);

	public abstract List<SqlItemPeer.ItemAndScore> retrieveMostPopularItems(int numItems, Set<Integer> dimensions);
	public abstract Map<String,Integer> getDimensionIdsForItem(long itemId);
}
