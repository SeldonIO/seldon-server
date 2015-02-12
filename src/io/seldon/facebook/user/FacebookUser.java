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

package io.seldon.facebook.user;

import com.restfb.Facebook;

/**
 * This is required as the default User type doesn't include some fields that we need to be mapped.
 * User: philipince
 * Date: 12/08/2013
 * Time: 14:23
 *
 */
public class FacebookUser {

    @Facebook
    Long uid;
    
    @Facebook
    Long uid2;
    //used for friends of user

    @Facebook("mutual_friend_count")
    Integer mutualFriendsCount;
    
    @Facebook("likes_count")
    Integer likesCount;

    public Boolean getIsAppUser() {
        return isAppUser;
    }

    @Facebook("is_app_user")
    Boolean isAppUser;

    public FacebookUser()
    {
    	
    }
    
    public FacebookUser(Long uid)
    {
    	this.uid = uid;
    }
    
    
    public Long getUid() {
        if (uid!=null)
        {
        	return uid;
        }
        else
        {
        	//if user is friend, fb api defines this as uid2...for us it is still a fb user
        	return uid2;
        }
    	
    }

    public Integer getMutualFriendsCount() {
        return mutualFriendsCount;
    }
    
    public Integer getLikesCount() {
        return likesCount;
    }

    public int hashCode(){
        return uid.intValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FacebookUser that = (FacebookUser) o;

        if (uid != null)
        {
        	if (uid != null ? !uid.equals(that.uid) : that.uid != null) return false;
        }
        else
        {
        	if (uid2 != null ? !uid2.equals(that.uid2) : that.uid2 != null) return false;
        }
        

        return true;
    }
}
