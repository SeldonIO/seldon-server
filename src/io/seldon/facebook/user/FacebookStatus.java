
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
 * Facebook Status class that maps to the Facebook status table.
 * User: dylanlentini
 * Date: 28/10/2013
 * Time: 16:27
 *
 */
public class FacebookStatus {

	//status_query = "SELECT status_id, time, source, message FROM status WHERE uid = me() limit 50"
	
	
    @Facebook("status_id")
    Long status_id;

    @Facebook("time")
    String time;

    @Facebook("source")
    String source;

    @Facebook("message")
    String message;

    
    public Long getStatusId() {
        return status_id;
    }

    public String getTime() {
        return time;
    }
    
    public String getSource() {
        return source;
    }
    
    public String getMessage() {
        return message;
    }
    
    
}
