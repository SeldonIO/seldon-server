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

import java.util.Collection;

public abstract class OpinionPeer {
	
	public abstract Opinion getOpinion(long itemId, long userId);	
	public abstract Collection<Opinion> getRecentOpinions(int limit);
	public abstract Collection<Opinion> getItemOpinions(long itemId,int limit);
	public abstract Collection<Opinion> getUserOpinions(long userId,int limit);
	public abstract long getNumSharedOpinions(long userId1,long userId2);
	public abstract long getNumOpinions(long userId);
	public abstract long getNumItemOpinions(long itemId);

}
