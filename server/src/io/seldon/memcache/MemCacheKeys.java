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

package io.seldon.memcache;

import io.seldon.recommendation.CFAlgorithm;
import io.seldon.util.CollectionTools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;


public class MemCacheKeys {


    private enum keys {
			UserTrustNetwork, ContentTrustNetwork, RummbleClient, SemanticVector, RecommendationNetwork,
			ConsumerBean,ItemBean,ItemsBeanNew,ItemSimilarityGraphBean,OpinionBean,OpinionsBean,RecommendationsBean,TokenBean,UserBean,UsersBean,UsterTrustGraphBean,DimensionBean, ActionBean,
			ActionsBean,ItemActionsBean,UserInternalId,ItemInternalId,UserClientId,ItemClientId,DimensionsBean,ItemsBeanByNameNew,ItemAttrType,ItemType,ActionType,ItemDimensions,InternalActionsBean,
			FacebookFriends,FacebookLikes,DBPediaSearch,RecommendedUsers,
			DimensionByItemType,DemographicBean,DBPediaHits, ItemSemanticAttribute, CooccurenceNetwork,
			ClusterCountForItems,ClustersForUser,TopClusterCounts,TopGlobalClusterCounts,TopClusterCountsForDimension,TopClusterCountsForTag,
			WebHitsForUser,ClusterCount,ClusterCountDecay,SharingRecommendation,ActionHistory,
			RankedItems,ABTesting,DynamicParameters,ShortTermClusters,
			SharingRecommendationsForItemSet, RecommendedItems, ExcludedItemsForRecommendations, RecommendationUUIDNew, RecommendationUUIDDim,
			RecentRecsForUsers, RecentItemsJSON, RecentItemsDimJSON, RecentItemsWithTagsJSON, ItemCluster, RecommendationUserMaxCounter, DBPediaHasBeenSearched, SocialPredictRecommendedItems,
			DimensionForAttrName,ItemTags,UserTags,ElphPrediction, itemRecommender, itemSimilarity, TagsForItem, TagItemCount, TagsItemCounts, SimilarUsers, InteractionBean, InteractionsBean,
            FacebookUsersAlgRecKey, FacebookUsersRecKey, FacebookUsersDecayFunctionKey, SharingRecommendationForKeywords, MostPopularItems,  PopularItemsJSON, ActionFullHistory, ExplicitItemsIncluder
			};

	
    public static String getUserTrustNetworkKey(String client,long user,int type)	{
    	return "" + keys.UserTrustNetwork + ":" + client + ":" + user + ":" + type;
    }
    
    public static String getRecommendationsKey(String client,long user,int type)	{
    	return "" + keys.RecommendationNetwork + ":" + client + ":" + type + ":" + user;
    }
    
    public static String getContentTrustNetworkKey(String client,long user,int type)	{
    	return "" + keys.ContentTrustNetwork + ":" + client + ":" + user + ":" + type;
    }
    
    public static String getRummbleClientKey(String name) {
    	return "" + keys.RummbleClient + ":" + name;
    }
    
    public static String getSemanticVectorKey(String client,String prefix,long user) {
    	return "" + keys.SemanticVector + ":" + client + ":" + prefix + ":" + user;
    }
    
			
    public static String getConsumerBeanKey(String client,String key) {
    	return "" + keys.ConsumerBean + ":" + client + ":" + key;
    }
    
  
    
    public static String getItemsBeanKey(String client, boolean full,String sort, int dimension) {
    	return "" + keys.ItemsBeanNew + ":" + client + ":" + full + ":" + sort + ":" + dimension;
    }
    
    public static String getItemsBeanKey(String client, String keywords, boolean full) {
    	return "" + keys.ItemsBeanNew + ":" + client + ":" + keywords + ":" + full;
    }

    public static String getItemBeanKey(String client, String id, boolean full) {
    	return "" + keys.ItemsBeanNew + ":" + client + ":" + id + ":" + full;
    }
    
    public static String getItemSimilarityGraphBeanKey(String client, String id) {
    	return "" + keys.ItemSimilarityGraphBean + ":" + client + ":" + id ;
    }
    

    
    public static String getTokenBeanKey(String key) {
    	return "" + keys.TokenBean + ":" + key;
    }
    
    public static String getUserBeanKey(String client,String id, boolean full) {
    	return "" + keys.UserBean + ":" + client + ":" + id + ":" + full;
    }
    
    public static String getUsersBeanKey(String client,boolean full) {
    	return "" + keys.UsersBean + ":" + client + ":" + full;
    }
    
    public static String getUserTrustGraphBeanKey(String client,String id) {
    	return "" + keys.UsterTrustGraphBean + ":" + client + ":" + id;
    }

    public static String getRecommendationsBeanKey(String client, CFAlgorithm algorithm,String id, String keywords, boolean full, int dimension) {
    	return "" + keys.RecommendationsBean + ":" + client + ":" +algorithm.toString()+":"+ id + ":" + keywords + ":" + full + ":" + dimension;
    }
    
	public static String getRecommendationsBeanKey(String client, CFAlgorithm algorithm,String id, Integer type,int dimensionId, boolean full) {
		return "" + keys.RecommendationsBean + ":" + client + ":" +algorithm.toString()+":"+ id + ":" + type + ":" + dimensionId + ":" + full;
	}

    public static String getRecommendedItemsKey(String client, CFAlgorithm algorithm, String userId, int typeId, int dimensionId, boolean full) {
        return "" + keys.RecommendedItems + ":" + client + ":" +algorithm.toString()+":"+ userId + ":" + typeId + ":" + dimensionId + ":" + full;
    }

    public static String getRecommendedItemsPerAlgKey(String client, String algName, Long user, int dimensionId){
        return "" + keys.RecommendedItems + ":" + client + ":" + algName + ":"+ user.toString() + ":" + dimensionId;
    }

	public static String getDimensionBeanKey(String client,int id) {
    	return "" + keys.DimensionBean + ":" + client + ":" + id;
	}
	
	public static String getDimensionBeanKey(String client,int attr,int val) {
    	return "" + keys.DimensionBean + ":" + client + ":" + attr + ":" + val;
	}
	
	public static String getDimensionBeanKey(String client,String attrName, String valName) {
    	return "" + keys.DimensionBean + ":" + client + ":" + attrName + ":" + valName;
	}
	
	public static String getDemographicBeanKey(String client,String attrName, String valName) {
    	return "" + keys.DemographicBean + ":" + client + ":" + attrName + ":" + valName;
	}
	
	public static String getActionBeanKey(String client,long actionId,boolean full, boolean ext) {
    	return "" + keys.ActionBean + ":" + client + ":" + ext + ":" + actionId;
	}
	
	public static String getUserActionsBeanKey(String client,String userId,boolean full,boolean ext) {
    	return "" + keys.ActionsBean + ":" + client + ":" + ":" + full + ":" + ext + ":" + userId;
	}
	
	public static String getUserItemActionBeanKey(String client,String userId,String itemId,boolean full, boolean ext) {
    	return "" + keys.ActionsBean + ":" + client + ":" + userId + ":" +  itemId + ":" + full + ":" + ext;
	}
	
	public static String getItemActionsBeanKey(String client,String itemId,boolean full, boolean ext) {
    	return "" + keys.ItemActionsBean + ":" + client + ":" + full + ":" + ext + ":" +  itemId;
	}
	
	public static String getUserInternalId(String client,String id) {
    	return "" + keys.UserInternalId + ":" + client + ":" + id;
	}
	
	public static String getItemInternalId(String client,String id) {
    	return "" + keys.ItemInternalId + ":" + client + ":" + id;
	}
	
	public static String getUserClientId(String client,long id) {
    	return "" + keys.UserClientId + ":" + client + ":" + id;
	}
	
	public static String getItemClientId(String client,long id) {
    	return "" + keys.ItemClientId + ":" + client + ":" + id;
	}

	public static String getDimensionsBeanKey(String client) {
		return "" + keys.DimensionsBean + ":" + client;
	}
	
	 public static String getItemsBeanKeyByName(String client, boolean full, String name, int dimension) {
	    	return "" + keys.ItemsBeanByNameNew + ":" + client + ":" + full + ":" + name + ":" + dimension;
	 }
	 
	 public static String getItemAttrType(String client, int itemType, String name) {
	    	return "" + keys.ItemAttrType + ":" + client + ":" + itemType + ":" + name;
	 }
	 
	 public static String getItemTypeByName(String client, String name) {
	    	return "" + keys.ItemType + ":" + client + ":" + name;
	 }
	 
	 public static String getItemTypeById(String client, int id) {
	    	return "" + keys.ItemType + ":" + client + ":" + id;
	 }
	 
	 public static String getActionTypeByName(String client, String name) {
	    	return "" + keys.ActionType + ":" + client + ":" + name;
	 }
	 
	 public static String getActionTypeById(String client, int id) {
	    	return "" + keys.ActionType + ":" + client + ":" + id;
	 }
	 
	 public static String getItemDimensions(String client, long itemId) {
	    	return "" + keys.ItemDimensions + ":" + client + ":" + itemId;
	 }
	 
	 public static String getItemCluster(String client, long itemId) {
	    	return "" + keys.ItemCluster + ":" + client + ":" + itemId;
	 }

	public static String getInternalUserItemActionBeanKey(String client, long userId, long itemId, boolean full, boolean ext) {
		return "" + keys.InternalActionsBean + ":" + client + ":" + userId + ":" +  itemId + ":" + full + ":" + ext;
	}

	public static String getDimensionBeanByItemTypeKey(String client,int itemType) {
		return "" + keys.DimensionByItemType + ":" + client + ":" + itemType;
	}
	public static String getFacebookFriends(String client,long userId)
	{
		return "" + keys.FacebookFriends + ":" + client + ":" + userId;
	}
	
	public static String getFacebookLikes(String client,long userId)
	{
		return "" + keys.FacebookLikes + ":" + client + ":" + userId;
	}
	
	
	public static String getDBPediaHits(String client,String query)
	{
		return "" + keys.DBPediaHits + ":" + client + ":" + query; 
	}
	
	public static String getRecommendedUsers(String client, String userId, String itemId, String linkType, String keywords) {
		return "" + keys.RecommendedUsers + ":" + client + ":" + userId + ":" + itemId + ":" + linkType + ":" + keywords;
	}

	public static String getUsersBeanKey(String client, boolean full,String name) {
		return "" + keys.UsersBean + ":" + client + ":" + full + ":" + name;
	}
	
	public static String getDemographicBeanKey(String client,int id) {
    	return "" + keys.DemographicBean + ":" + client + ":" + id;
	}

	 public static String getActionType(String client, int id) {
	    return "" + keys.ActionType + ":" + client + ":" + id;
	 }
	 
	 public static String getActionTypes(String client) {
		 return "" + keys.ActionType + ":" + client;
	 }
	 
	 public static String getItemTypes(String client) {
		 return "" + keys.ItemType + ":" + client;
	 }

	public static String getItemSemanticAttributes(String client, long itemId) {
		return "" + keys.ItemSemanticAttribute + ":" + client +  ":" + itemId;
	}

	public static String getCooccurenceNetworkKey(String client, long user) {
		return "" + keys.CooccurenceNetwork + ":" + client +  ":" + user;
	}
	
	public static String getClusterCountForItems(String client,int clusterId,List<Long> items,long version)
	{
		ArrayList<Long> itemKeys = new ArrayList<>(items);
		Collections.sort(itemKeys); // to provide consistent order
		StringBuffer b = new StringBuffer(""+keys.ClusterCountForItems);
		b.append(":");
		b.append(client);
		b.append(":");
		b.append(clusterId);
		b.append(":");
		b.append(version);
		for(Long item : itemKeys)
			b.append(":").append(item);
		return b.toString();
	}
	
	public static String getClustersForUser(String client,long userId)
	{
		return ""+keys.ClustersForUser+":"+client+":"+userId;
	}
	
	public static String getShortTermClustersForUser(String client,long userId)
	{
		return ""+keys.ShortTermClusters+":"+client+":"+userId;
	}
	
	public static String getWebHitsForUser(String client,long userId,String linkType)
	{
		return ""+keys.WebHitsForUser+":"+client+":"+userId+":"+linkType;
	}
	
	public static String getClusterCountKey(String client,long clusterId,long itemId,long clusterTimestamp)
	{
		return ""+keys.ClusterCount+":"+client+":"+clusterId+":"+itemId+":"+clusterTimestamp;
	}
	
	public static String getClusterCountDecayKey(String client,long clusterId,long itemId,long clusterTimestamp)
	{
		return ""+keys.ClusterCountDecay+":"+client+":"+clusterId+":"+itemId+":"+clusterTimestamp;
	}
	
	public static String getSharingRecommendationKey(String client,long userId,long itemId,String linkType)
	{
		return ""+keys.SharingRecommendation+":"+client+":"+userId+":"+itemId+":"+linkType;
	}
	
	public static String getSharingRecommendationKey(String client,long userId,List<Long> items)
	{
		ArrayList<Long> itemKeys = new ArrayList<>(items);
		Collections.sort(itemKeys);
		return ""+keys.SharingRecommendation+":"+client+":"+userId+":"+CollectionTools.join(itemKeys, ",");
	}
	
	public static String getActionHistory(String client,long userId)
	{
		return ""+keys.ActionHistory+":"+client+":"+userId;
	}

	public static String getActionFullHistory(String client,long userId)
	{
		return ""+keys.ActionFullHistory+":"+client+":"+userId;
	}

	
	public static String getRankedItemsKey(String client,long cfalgorithm, String userId,List<String> items)
	{
		ArrayList<String> itemKeys = new ArrayList<>(items);
		Collections.sort(itemKeys);
		return ""+keys.RankedItems+":"+client+":" + cfalgorithm + ":"+userId+":"+CollectionTools.join(itemKeys, ",");
	}

	public static String getABTestingUser(String consumer, String clientUserId, String testKey) {
		return ""+keys.ABTesting+":"+consumer+":"+testKey+":"+clientUserId;
	}
	
	public static String getDynamicParameters(String consumer) {
		return ""+keys.DynamicParameters+":"+consumer;
	}
	
	public static String getSharingRecommendationsForItemSetKey(String client,long userId)
	{
		return ""+keys.SharingRecommendationsForItemSet.name()+":"+client+":"+userId;
	}
	
	public static String getTopClusterCounts(String client,int clusterId,int limit)
	{
		return ""+keys.TopClusterCounts.name()+":"+client+":"+clusterId+":"+limit;
	}
	
	public static String getTopClusterCounts(String client,int limit)
	{
		return ""+keys.TopGlobalClusterCounts.name()+":"+client+":"+limit;
	}
	
	public static String getTopClusterCountsForDimension(String client,int clusterId,Set<Integer> dimensions,int limit)
	{
		return ""+keys.TopClusterCounts.name()+":"+client+":"+clusterId+":"+StringUtils.join(dimensions, ",")+":"+limit;
	}
	
	public static String getTopClusterCountsForTwoDimensions(String client,int clusterId,Set<Integer> dimensions,int dim2,int limit)
	{
		return ""+keys.TopClusterCounts.name()+":"+client+":"+clusterId+":"+StringUtils.join(dimensions, ",")+":"+dim2+":"+limit;
	}
	
	public static String getTopClusterCountsForDimensionAlg(String client,String alg,int clusterId,Set<Integer> dimensions,int limit)
	{
		return ""+keys.TopClusterCounts.name()+":"+client+":"+alg+":"+clusterId+":"+StringUtils.join(dimensions, ",")+":"+limit;
	}
	
	public static String getTopClusterCountsForTagAndDimension(String client,String tag,int tagAttrId,Set<Integer> dimensions,int limit)
	{
		return ""+keys.TopClusterCountsForTag.name()+":"+client+":"+tag+":"+tagAttrId+":"+StringUtils.join(dimensions, ",")+":"+limit;
	}

	public static String getTopClusterCountsForTagAndTwoDimensions(String client,String tag,int tagAttrId,Set<Integer> dimensions,int dimension2,int limit)
	{
		return ""+keys.TopClusterCountsForTag.name()+":"+client+":"+tag+":"+tagAttrId+":"+StringUtils.join(dimensions, ",")+":"+dimension2+":"+limit;
	}
	
	public static String getTopClusterCountsForTag(String client,String tag,int tagAttrId,int limit)
	{
		return ""+keys.TopClusterCountsForTag.name()+":"+client+":"+tag+":"+tagAttrId+":"+limit;
	}
	

	
	public static String getTopClusterCountsForDimension(String client,Set<Integer> dimensions,int limit)
	{
		return ""+keys.TopGlobalClusterCounts.name()+":"+client+":"+StringUtils.join(dimensions, ",")+":"+limit;
	}
	
	public static String getTopClusterCountsForTwoDimensions(String client,Set<Integer> dimensions,int dimension2,int limit)
	{
		return ""+keys.TopGlobalClusterCounts.name()+":"+client+":"+StringUtils.join(dimensions, ",")+":"+dimension2+":"+limit;
	}
	
	public static String getExcludedItemsForRecommendations(String client,String userId)
	{
		return ""+keys.ExcludedItemsForRecommendations.name()+":"+client+":"+userId;
	}
	
	public static String getRecommendationListUUID(String client,String userId,int counter, String recTag)
	{
		return ""+keys.RecommendationUUIDNew.name()+":"+client+":"+userId+":"+counter + ":" + recTag;
	}
	
	public static String getRecommendationListUUIDWithDimension(String client,int dimension, String userId,int counter)
	{
		return ""+keys.RecommendationUUIDDim.name()+":"+client+":"+userId+":"+dimension+":"+counter;
	}
	
	public static String getRecommendationListUserCounter(String client,Set<Integer> dimensions, String userId)
	{
		return ""+keys.RecommendationUserMaxCounter.name()+":"+client+":"+userId+":"+StringUtils.join(dimensions, ",");
	}
	
	public static String getRecentRecsForUser(String client,String userId,Set<Integer> dimensions)
	{
		return ""+keys.RecentRecsForUsers.name()+":"+client+":"+userId+":"+StringUtils.join(dimensions, ",");
	}
	
	public static String getRecentItems(String client,Set<Integer> dimensions,int size)
	{
		return ""+keys.RecentItemsJSON.name()+":"+client+":"+StringUtils.join(dimensions, ",")+":"+size;
	}
	
	public static String getRecentItemsInDimension(String client,Set<Integer> dimensions,int dimId,int size)
	{
		return ""+keys.RecentItemsDimJSON.name()+":"+client+":"+StringUtils.join(dimensions, ",")+":"+dimId+":"+size;
	}
	
	public static String getRecentItemsWithTags(String client,int tagAttrId,int tagKey,int size)
	{
		return ""+keys.RecentItemsWithTagsJSON.name()+":"+client+":"+":"+tagAttrId+":"+tagKey+":"+size;
	}

	public static String getPopularItems(String client, Set<Integer> dimensions, int size){
		return ""+keys.PopularItemsJSON.name()+":"+client+":"+StringUtils.join(dimensions, ",")+":"+size;
	}
	
	public static String getDbpediaHasBeenSearched(String client,long itemId)
	{
		return ""+keys.DBPediaHasBeenSearched.name()+":"+client+":"+itemId;
	}
	
	public static String getSocialPredictRecommendedItems(String client,long userId,long itemsHashCode)
	{
		return ""+keys.SocialPredictRecommendedItems.name()+":"+client+":"+userId+":"+itemsHashCode;
	}
	
	public static String getDimensionForAttrName(String client,long itemId,String category)
	{
		return ""+keys.DimensionForAttrName+":"+client+":"+itemId+":"+category;
	}
	
	public static String getItemTags(String client,int numItems,int dimension)
	{
		return ""+keys.ItemTags.name()+":"+client+":"+numItems+":"+dimension;
	}

	public static String getUserTags(String client,long userId)
	{
		return ""+keys.UserTags.name()+":"+client+":"+userId;
	}
	
	public static String getElphPrediction(String client,List<Long> recentItems)
	{
		return ""+keys.ElphPrediction+":"+client+":"+CollectionTools.join(recentItems, ",");
	}
	
	public static String getItemRecommender(String client,long userId,int dimension,int max)
	{
		return ""+keys.itemRecommender+":"+client+":"+userId+":"+dimension+":"+max;
	}
	
	public static String getItemSimilarity(String client,long itemId,Set<Integer> dimensions,int max)
	{
		return ""+keys.itemSimilarity+":"+client+":"+itemId+":"+StringUtils.join(dimensions, ",")+":"+max;
	}
	
	public static String getItemTags(String client,long itemId,int attrId)
	{
		return ""+keys.TagsForItem+":"+client+":"+itemId+":"+attrId;
	}
	
	public static String getTagsItemCounts(String client,Set<String> tags,int dimension)
	{
		ArrayList<String> tlist = new ArrayList<>(tags);
		Collections.sort(tlist);
		String normalizedTagKey = CollectionTools.join(tlist, ":");
		return ""+keys.TagsItemCounts+":"+client+":"+dimension+":"+ normalizedTagKey;
	}
	
	public static String getTagItemCount(String client,String tag,int dimension)
	{
		return ""+keys.TagItemCount+":"+client+":"+dimension+":"+tag;
	}
	
	public static String getSimilarUsers(String client,long userId,int type,int filterType)
	{
		return ""+keys.SimilarUsers+":"+client+":"+userId+":"+type+":"+filterType;
	}
	public static String getSharingRecommendationForKeywords(String client,long userId,List<String> keywords)
    {
        ArrayList<String> keywordSet = new ArrayList<>(keywords);
        Collections.sort(keywordSet);
        return ""+keys.SharingRecommendationForKeywords+":"+client+":"+userId+CollectionTools.join(keywordSet, ",");
    }

    public static String getInteractionBeanKey(String client, String user1, String user2, int type, int subType) {
        return ""+ keys.InteractionBean+":"+user1+":"+user2+":"+type+":"+subType;
    }
    
    public static String getInteractionsBeanKey(String client, String user1, int type){
        return ""+ keys.InteractionsBean+":"+user1 +":"+type;
    }

    public static String getFacebookUsersAlgRecKey(String service, String client, String algName, String userId, int algParams) {
        return "" + keys.FacebookUsersAlgRecKey+":"+service+":"+client+":"+algName+":"+userId+":"+algParams;
    }

    public static String getFacebookUsersRecKey(String service, String client, String userId, Integer strategyCode, int algParamsCode) {
        return "" + keys.FacebookUsersRecKey + ":" + service + ":" + client + ":" + userId + ":" + strategyCode + ":" + algParamsCode;
    }

    public static String getFacebookUsersDecayFunctionKey(String service, String client, String userId, String decayFunctionType) {
        return "" + keys.FacebookUsersDecayFunctionKey + ":" + service + ":" + client + ":" + userId + ":" + decayFunctionType;
    }

    public static String getFacebookUsersImpressionsKey(String decayFunctionType, String service, String client, String userId) {
        return "" + keys.FacebookUsersDecayFunctionKey + ":" + decayFunctionType + ":" + service + ":" + client + ":" + userId;
    }

    public static String getMostPopularItems(String client,int dimension)
    {
    	return ""+keys.MostPopularItems+":"+client+":"+dimension;
    }
    
    public static String getMostPopularItems(String client)
    {
    	return ""+keys.MostPopularItems+":"+client;
    }

    public static String getExplicitItemsIncluderKey(String client,Set<Long> items)
    {
    	return ""+keys.ExplicitItemsIncluder+":"+client+":"+items.hashCode();
    }
    
}

