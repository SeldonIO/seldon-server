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
import java.util.List;

import io.seldon.facebook.user.FacebookUsersAlgorithm;

public class SocialRecommendationStrategy
{

    public enum StrategyAim {
        SIGNUP_NEW_MEMBERS, SHARE_PAGES;
    }

    protected StrategyAim aim;
    protected List<FacebookUsersAlgorithm> onlineAlgorithms;
    protected List<FacebookUsersAlgorithm> offlineAlgorithms;
    protected List<FacebookUsersAlgorithm> inclusiveFilters;
    protected List<FacebookUsersAlgorithm> exclusiveFilters;
    protected SocialFriendsBlender blender;
    protected List<SocialFriendsScoreDecayFunction> decayFunctions;
    protected Integer uniqueCode;

	
	public SocialRecommendationStrategy() 
	{
		onlineAlgorithms  = new ArrayList<FacebookUsersAlgorithm>();
		offlineAlgorithms = new ArrayList<FacebookUsersAlgorithm>();
        inclusiveFilters  = new ArrayList<FacebookUsersAlgorithm>();
        exclusiveFilters  = new ArrayList<FacebookUsersAlgorithm>();
        decayFunctions    = new ArrayList<SocialFriendsScoreDecayFunction>();
        uniqueCode = 0;
        // default is share pages
        aim = StrategyAim.SHARE_PAGES;
	}


    public List<FacebookUsersAlgorithm> getOnlineAlgorithms()
    {
        return onlineAlgorithms;
    }

    public void setOnlineAlgorithms(List<FacebookUsersAlgorithm> onlineAlgorithms)
    {
        this.onlineAlgorithms = onlineAlgorithms;
    }


    public List<FacebookUsersAlgorithm> getOfflineAlgorithms()
    {
        return offlineAlgorithms;
    }

    public void setOfflineAlgorithms(List<FacebookUsersAlgorithm> offlineAlgorithms)
    {
        this.offlineAlgorithms = offlineAlgorithms;
    }



    public List<FacebookUsersAlgorithm> getInclusiveFilters()
    {
        return inclusiveFilters;
    }

    public void setInclusiveFilters(List<FacebookUsersAlgorithm> inclusiveFilters)
    {
        this.inclusiveFilters = inclusiveFilters;
    }


    public List<FacebookUsersAlgorithm> getExclusiveFilters()
    {
        return exclusiveFilters;
    }

    public void setExclusiveFilters(List<FacebookUsersAlgorithm> exclusiveFilters)
    {
        this.exclusiveFilters = exclusiveFilters;
    }


	public SocialFriendsBlender getBlender() 
	{
		return blender;
	}
	
	public void setBlender(SocialFriendsBlender blender) 
	{
		this.blender = blender;
	}


    public List<SocialFriendsScoreDecayFunction> getDecayFunctions()
    {
        return decayFunctions;
    }

    public void setDecayFunctions(List<SocialFriendsScoreDecayFunction> decayFunctions)
    {
        this.decayFunctions = decayFunctions;
    }


    public StrategyAim getAim() {
        return aim;
    }

    public void setAim(StrategyAim aim) {
        this.aim = aim;
    }


    public Integer getUniqueCode() {
        return uniqueCode;
    }

    public void setUniqueCode(String strategyString) {
        this.uniqueCode = strategyString.hashCode();
    }



    public static SocialRecommendationStrategy build(List<FacebookUsersAlgorithm> onlineAlgs,
                                                     List<FacebookUsersAlgorithm> offlineAlgs,
                                                     List<FacebookUsersAlgorithm> incFilters,
                                                     List<FacebookUsersAlgorithm> excFilters,
                                                     SocialFriendsBlender blender,
                                                     List<SocialFriendsScoreDecayFunction> decayFunctions,
                                                     StrategyAim aim,
                                                     String strategyString)
    {
        SocialRecommendationStrategy toReturn = new SocialRecommendationStrategy();
        if(onlineAlgs!=null) toReturn.setOnlineAlgorithms(onlineAlgs);
        if(offlineAlgs!=null) toReturn.setOfflineAlgorithms(offlineAlgs);
        if(incFilters!=null) toReturn.setInclusiveFilters(incFilters);
        if(excFilters!=null) toReturn.setExclusiveFilters(excFilters);
        if(blender!=null) toReturn.setBlender(blender);
        if(decayFunctions!=null) toReturn.setDecayFunctions(decayFunctions);
        if (aim!=null) toReturn.setAim(aim);
        if (strategyString!=null) toReturn.setUniqueCode(strategyString);
        return toReturn;
    }
	
}
