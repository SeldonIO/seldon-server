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

package io.seldon.facebook;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import io.seldon.api.resource.RecommendedUserBean;
import org.springframework.stereotype.Component;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.seldon.api.resource.ListBean;

@Component
public class SocialFriendsRankBlender implements SocialFriendsBlender {
	
	@Override
	public List<RecommendedUserBean> blendFriends(List<List<RecommendedUserBean>> rankedFriendsLists)
	{
		int maxFriendSize = 0;
		for (List<RecommendedUserBean> algFriends : rankedFriendsLists)
		{
			maxFriendSize = algFriends.size() > maxFriendSize ? algFriends.size() : maxFriendSize;
		}
		
		int i = 0;
		for (List<RecommendedUserBean> algFriends : rankedFriendsLists)
		{
			i = 0;
			for (RecommendedUserBean friendBean : algFriends)
			{
				friendBean.setScore((double) maxFriendSize - i);
				i++;
			}
			
			
		}
		
		
		Map<String,RecommendedUserBean> combinedFriends = new HashMap<String,RecommendedUserBean>();
		
		for (List<RecommendedUserBean> algFriends : rankedFriendsLists)
		{
			i = 0;
			for (RecommendedUserBean friendBean : algFriends)
			{
				if (combinedFriends.containsKey(friendBean.getUser()))
				{
					RecommendedUserBean friendUpdate = combinedFriends.get(friendBean.getUser());
					double newScore = friendUpdate.getScore() + friendBean.getScore();
					friendUpdate.setScore(newScore);
					combinedFriends.put(friendBean.getUser(), friendUpdate);
				}
				else
				{
					combinedFriends.put(friendBean.getUser(), friendBean);
				}
			}
		}
		
		
		List<RecommendedUserBean> rankedFriends = new ArrayList<RecommendedUserBean>(combinedFriends.values());
		
		return rankedFriends;
		
	}
	
	
	
	
	//currently being used by offline algs
	public ListBean rankRecommendationsOfflineMode(Set<List<RecommendedUserBean>> recommended) {
        ListBean toReturn = new ListBean();
        if(recommended == null || recommended.size() == 0) return toReturn;
        if(recommended.size() == 1){
            toReturn.addAll(recommended.iterator().next());
            return toReturn;
        }
        Map<String, RecommendedUserBean> userToBeanExample = new HashMap<String, RecommendedUserBean>();
        Map<String, Double> userToCount = new HashMap<String, Double>();
        Map<String, Set<String>> userToReasons = new HashMap<String, Set<String>>();

        int maxSize = 0;
        for(List<RecommendedUserBean> l : recommended){
            maxSize = Math.max(l.size(), maxSize);
        }
        if(maxSize==0) return toReturn;

        for(List<RecommendedUserBean> l : recommended){
            if(l.size()==0) continue;
            double rankingScoreStep = maxSize / l.size();
            double currentScore = maxSize;
            for(RecommendedUserBean rec : l){
                Double previousScore = userToCount.get(rec.getUser());
                if(previousScore==null){
                    previousScore = 0D;
                    userToBeanExample.put(rec.getUser(), rec);
                    userToReasons.put(rec.getUser(), Sets.newHashSet(rec.getReasons()));
                } else { userToReasons.get(rec.getUser()).addAll(rec.getReasons()); }

                double score = currentScore + (previousScore!=null ? previousScore : 0);
                userToCount.put(rec.getUser(), score);
                currentScore -= rankingScoreStep;
            }
        }
        SortedMap<String, Double> userToCountSorted = ImmutableSortedMap.copyOf(userToCount, Ordering.natural().onResultOf(Functions.forMap(userToCount)));
        Double maxScore = userToCountSorted.get(userToCountSorted.lastKey());
        while(!userToCountSorted.isEmpty()){
            String thisKey = userToCountSorted.lastKey();
            RecommendedUserBean bean = userToBeanExample.get(thisKey);
            bean.setScore(userToCountSorted.get(thisKey) / maxScore);
            bean.setReasons(new ArrayList<String>(userToReasons.get(thisKey)));
            userToCountSorted = userToCountSorted.headMap(thisKey);
            toReturn.addBean(bean);
        }
        return toReturn;
    }


}
