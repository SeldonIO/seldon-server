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

package io.seldon.memcache;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class DogpileHandler {
	private static Logger logger = Logger.getLogger(DogpileHandler.class.getName());
	
	private static final float DEF_EXPIRE_FACTOR = 0.75f;
	public static final String DOGPILE_EXPIRE_PROP = "io.seldon.dogpile.expire.factor";
	public static final String DOGPILE_ACTIVE = "io.seldon.dogpile.active";
	
	private static DogpileHandler peer;

    public  DogpileHandler ()
    {
		expireFactor = DEF_EXPIRE_FACTOR;
		active = true;
		logger.info("Creating dogpile cache handler with active: "+active+" and expire factor "+expireFactor);
		peer = this;
    }
	
	public static DogpileHandler get()
	{
		return peer;
	}
	
	ConcurrentHashMap<String,Long> expireMap = new ConcurrentHashMap<>();
	ConcurrentHashMap<String,Boolean> updatingMap = new ConcurrentHashMap<>();
	float expireFactor;
	boolean active = false;
	private DogpileHandler(float expireFactor,boolean active)
	{
		this.expireFactor = expireFactor;
		this.active = active;
	}
	
	private boolean setUpdating(String key)
	{
		if (updatingMap.putIfAbsent(key, true) == null)
		{
			// if there was not updating flag then return true to tell caller an update is needed
			//
			// There is a possibility of a race condition of 1 (or more) thread checks expire then gets stopped and another updates
			// then first (or more) thread(s) comes back in and set updateingMap successfully. However this should only happen under extreme load
			// and will cause a few extra updates
			logger.info("Returning true for memcache key "+key);
			return true;
		}
		else
			return false; // as another thread is updating the value
	}

	public <T> T retrieveUpdateIfRequired(String key, T cachedItem, UpdateRetriever<T> retriever, int expireSecs) throws Exception {

		if(updateIsRequired(key, cachedItem, expireSecs)){
			try {
				return retriever.retrieve();
			} finally {
				updated(key, expireSecs );
			}
		} else {
			return null;
		}
	}

	public <T> boolean updateIsRequired(String key, T cachedItem, int expireSecs){
		boolean continueWithUpdate = false;
		if (key == null || !active) return false;
		if (cachedItem==null) {
			continueWithUpdate = setUpdating(key);
		} else {
			Long expire = expireMap.get(key);
			if (expire == null || (expire !=null && System.currentTimeMillis() > expire))
			{
				continueWithUpdate = setUpdating(key);
			}
		}
		return continueWithUpdate;
	}

	public void updated(String key,int expireSecs)
	{
		if (!active) return;
		logger.info("Updating key "+key);
		int expireDeltaMilisecs = Math.round((expireSecs * 1000) * expireFactor);
		expireMap.put(key, System.currentTimeMillis() + expireDeltaMilisecs);
		updatingMap.remove(key);
	}
	
	public void clear(String key)
	{
		expireMap.remove(key);
		updatingMap.remove(key);
	}
}
