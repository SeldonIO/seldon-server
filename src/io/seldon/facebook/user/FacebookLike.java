
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
 * Facebook Like class that maps to the Facebook like table.
 * User: dylanlentini
 * Date: 28/10/2013
 * Time: 17:00
 *
 */
public class FacebookLike {

	//user_status_like_query = "SELECT user_id,object_id FROM like WHERE object_id IN (SELECT status_id FROM #status_query)"
	
    @Facebook("user_id")
    Long user_id;

    @Facebook("object_id")
    Long object_id;

    public Long getUserId() {
        return user_id;
    }
    
    public Long getObjectId() {
        return object_id;
    }

    
}
