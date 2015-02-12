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
import java.util.Map;
import io.seldon.api.resource.ConsumerBean;

public abstract class UserPeer {
	
	public abstract User getUser(long userId);
	public abstract User getUser(String userId);	
	public abstract Collection<User> getRecentUsers(int limit);
	public abstract Double getUserAvgRating(long userId,int dimension);
    public abstract User saveOrUpdate(final User user);
	public abstract User persistUser(User u);
	public abstract Collection<User> getActiveUsers(int limit);
	public abstract Collection<User> findUsersWithAttributeValuePair(Integer attributeId, Object value, String type);
	public abstract Integer[] getAttributes(int demographic);
	public abstract String[] getAttributesNames(int demographic);
	public abstract boolean addUserAttribute(long userId, int userType, Map<Integer, Integer> attributes,ConsumerBean c);
	public abstract boolean addUserAttributeNames(long userId, int userType, Map<String, String> attributes, ConsumerBean c);
}
