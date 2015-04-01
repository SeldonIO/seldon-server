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

public abstract class NetworkPeer {
	
	public abstract boolean addExplicitLink(ExplicitLink link);
	public abstract LinkType getLinkType(int id);
	public abstract LinkType getLinkType(String name);
	public abstract boolean addLinkType(LinkType l);
	public abstract Collection<User> getLinkedUsers(long userId,String linkType,int limit);
	public abstract ExplicitLink getExplicitLink(long u1, long u2, int type);

}
