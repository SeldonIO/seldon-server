
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
 * Facebook Permissions class lists all permissions we need to get information from Facebook. Facebook Permissions class that maps to the Facebook permissions table.
 * User: dylanlentini
 * Date: 31/10/2013
 * Time: 13:47
 *
 */
public class FacebookPermissions {

	@Facebook("user_status")
    Integer user_status_permission;

    @Facebook("user_photos")
    Integer user_photos_permission;

    @Facebook("user_friends")
    Integer user_friends_permission;

    
    public Integer getUserStatusPermission() {
        return user_status_permission;
    }
    
    public Integer getUserPhotosPermission() {
        return user_photos_permission;
    }
    
    public Integer getUserFriendsPermission() {
        return user_friends_permission;
    }

    
}
