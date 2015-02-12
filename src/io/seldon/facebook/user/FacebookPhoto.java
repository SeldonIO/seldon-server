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
 * Facebook Photo class that maps to the Facebook photo table.
 * User: dylanlentini
 * Date: 28/10/2013
 * Time: 16:35
 *
 */
public class FacebookPhoto {

	//photo_query = "SELECT object_id, created, caption FROM photo WHERE owner = me() limit 50"
	
    @Facebook("object_id")
    Long object_id;

    @Facebook("created")
    String created;

    @Facebook("caption")
    String caption;

    
    
    public Long getObjectId() {
        return object_id;
    }

    public String getCreated() {
        return created;
    }
    
    public String getCaption() {
        return caption;
    }
    
    
    
}
