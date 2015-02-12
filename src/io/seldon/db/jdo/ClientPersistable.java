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

package io.seldon.db.jdo;

import javax.jdo.PersistenceManager;

import io.seldon.api.APIException;
import org.apache.log4j.Logger;

public class ClientPersistable {
	private static Logger logger = Logger.getLogger( ClientPersistable.class.getName() );
	
	protected String clientName;
	private PersistenceManager pm;
	
	public ClientPersistable(String clientName)
	{
		this.clientName = clientName;
		this.pm = null;
	}
	
	/*
	 * Lazy get of PM
	 * @return
	 */
	public PersistenceManager getPM()
	{
		if (pm == null)
		{
			pm = JDOFactory.getPersistenceManager(clientName);
			if(pm == null) {
    			throw new APIException(APIException.INTERNAL_DB_ERROR);
    		}
		}
		return pm;
	}
	
}
