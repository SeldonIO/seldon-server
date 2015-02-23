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

package io.seldon.test.peer;

import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.clustering.recommender.TransientUserClusterStore;
import io.seldon.clustering.recommender.UserClusterStore;
import io.seldon.general.ActionPeer;
import io.seldon.general.ItemPeer;
import io.seldon.general.NetworkPeer;
import io.seldon.general.UserAttributePeer;
import io.seldon.general.UserPeer;
import io.seldon.general.VersionPeer;
import io.seldon.test.BaseTest;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: marc on 05/08/2011 at 13:21
 */
public abstract class BasePeerTest extends BaseTest {

    @Autowired
    protected UserPeer userPeer;

    @Autowired
    protected UserAttributePeer userAttributePeer;

    @Autowired
    protected ItemPeer itemPeer;

    @Autowired 
    protected ActionPeer actionPeer;
    
    @Autowired
    protected VersionPeer versionPeer;
    
    @Autowired
    protected NetworkPeer networkPeer;
    
    //@Autowired
    //protected WebSearchResultsPeer webSearchResultsPeer;

    //@Autowired
	//protected StopWordPeer stopWordPeer;
    
    //@Autowired
	//protected DBpediaIndex index;
	
    @Autowired
    protected ClusterCountStore clusterCount;
    
    @Autowired
    protected UserClusterStore userClusters;
    
    @Autowired
    protected TransientUserClusterStore transientClusterStore;
    
    
    
   
}
