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

package io.seldon.api.state.jdo;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.state.IHostState;
import io.seldon.db.jdo.JDOFactory;
import org.apache.log4j.Logger;

import io.seldon.api.Constants;

public class SqlHostStatePeer implements IHostState {

	private static Logger logger = Logger.getLogger( SqlHostStatePeer.class.getName() );
	
	@Override
	public boolean canRunActionUpdates(String hostname) {
		
		try {
			PersistenceManager pm =  JDOFactory.getPersistenceManager(Constants.API_DB);
			Query query = pm.newQuery( "javax.jdo.query.SQL", "select action_update from hosts where hostname=?");
			query.setUnique(true);
			Boolean runActionUpdate = (Boolean) query.execute(hostname);
			if (runActionUpdate != null)
				return runActionUpdate;
			else
			{
				logger.info("Failed to find hostname in hosts table where hostname is "+hostname+" will return true for safety");
				return true;
			}
		}
		catch(Exception e) {
			logger.error("Failed to get action_update from hosts table will return true for safety",e);
			return true;
		}
	}

	

}
