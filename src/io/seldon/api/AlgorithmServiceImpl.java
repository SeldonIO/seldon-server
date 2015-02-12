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

package io.seldon.api;

import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountStore;
import io.seldon.clustering.tag.AsyncTagClusterCountFactory;
import io.seldon.trust.impl.CFAlgorithm;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Created by: marc on 22/11/2011 at 13:38
 */
public class AlgorithmServiceImpl implements AlgorithmService {
	private static Logger logger = Logger.getLogger(AlgorithmServiceImpl.class.getName());
	
    private ConcurrentHashMap<String, CFAlgorithm> algorithmMap;

    public void setAlgorithmMap(ConcurrentHashMap<String, CFAlgorithm> algorithmMap) {
        this.algorithmMap = algorithmMap;
    }

    @Qualifier("algorithm.cfAlgorithm.default")
    @Autowired
    private CFAlgorithm defaultCFAlgorithm;

    @Override
	public CFAlgorithm getAlgorithmOptions(ConsumerBean consumerBean)
			throws CloneNotSupportedException {
		return getAlgorithmOptions(consumerBean, null);
	}

	@Override
	public CFAlgorithm getAlgorithmOptions(String consumerName)
			throws CloneNotSupportedException {
		return getAlgorithmOptions(consumerName, null);
	}
    
    @Override
    public CFAlgorithm getAlgorithmOptions(ConsumerBean consumerBean,String recTag) throws CloneNotSupportedException {
        final String consumerShortName = consumerBean.getShort_name();
        return getAlgorithmOptions(consumerShortName,recTag);
    }
    
    private String getKey(String consumerShortName,String recTag)
    {
    	return recTag == null ? consumerShortName : consumerShortName+":"+recTag;
    }

    @Override
    public CFAlgorithm getAlgorithmOptions(String consumerShortName,String recTag) throws CloneNotSupportedException {
    	final String key = getKey(consumerShortName, recTag);
        final CFAlgorithm cfAlgorithm = algorithmMap.get(key);
        if (cfAlgorithm == null && recTag != null)
        {
        	logger.warn("Could not find entry for consumer "+consumerShortName+" and recTag "+recTag+" will default back to one for consumer with no tag");
        	return getAlgorithmOptions(consumerShortName, null);
        }
        else if (cfAlgorithm == null) {
            CFAlgorithm cloneOfDefault = defaultCFAlgorithm.clone();
            cloneOfDefault.setName(consumerShortName);
            algorithmMap.put(key, cloneOfDefault);
            return cloneOfDefault;
        }
        // ensure the name is set
        if ( cfAlgorithm.getName() == null ) {
            cfAlgorithm.setName(consumerShortName);
        }
        return cfAlgorithm;
    }

	@Override
	public void setAlgorithmOptions(String consumerName, CFAlgorithm algorithm) {
		final String key = getKey(consumerName, algorithm.getRecTag());
		algorithmMap.put(key, algorithm);
		if (algorithm.getRecTag() == null) // default algorithm
		{
			AsyncClusterCountFactory cFac = AsyncClusterCountFactory.get();
			if (cFac != null)
			{
				AsyncClusterCountStore asyncStore = cFac.get(consumerName);
				if (asyncStore != null)
				{
					asyncStore.setDecay(algorithm.getDecayRateSecs());
				}
				AsyncTagClusterCountFactory fac = AsyncTagClusterCountFactory.get();
				if (fac != null)
					fac.setActive(consumerName, algorithm.isTagClusterCountsActive());
			
			}
		}
	}

	


}
