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

package io.seldon.clustering.minhash.jdo;

import io.seldon.clustering.minhash.ItemFrequencyHandler;
import io.seldon.general.OpinionPeer;

/**
 * Item frequency peer
 * @author rummble
 *
 */
public class ItemFrequencyPeer implements ItemFrequencyHandler {

	OpinionPeer o;
	
	public ItemFrequencyPeer(OpinionPeer o)
	{
		this.o = o;
	}
	
	
	@Override
	public long getFrequency(String item) {
		return o.getNumItemOpinions(Long.parseLong(item));
	}

}
