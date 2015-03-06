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

/*
 * Created on Jan 8, 2006
 *
 */
package io.seldon.db.jdo;

import java.util.HashMap;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.apache.log4j.Logger;

public class JDOPMRetriever extends ThreadLocal<Map<String,PersistenceManager>>  
{
    private static Logger logger = Logger.getLogger( JDOPMRetriever.class.getName() );
    public void cleanup() {
    	try
    	{
    		Map<String,PersistenceManager> map = (Map<String,PersistenceManager>)super.get();
    		if (map != null)
        	for(PersistenceManager pm : map.values())
        	{
        		if(pm == null) return;

        		try 
        		{
        			if(!pm.isClosed()) 
               	 	{
        				TransactionPeer.closeReadOnlyTransaction(pm);
        				Transaction ts = pm.currentTransaction();
        				if(ts.isActive()) 
        				{
                       	 	logger.warn("transaction stil active");
                       	 	ts.rollback();
                    	}
        				pm.close();
                	}
        			else
        			{
        				logger.warn("pm is closed");
        			}

        		} 
        		catch(Exception ex) 	
        		{
        			logger.warn("exception on cleanup",ex);
        		} 
        	}
    	
    	}
        finally {
            set(null);
        }

    }

   
    
    public  Object getPersistenceManager(String client,PersistenceManagerFactory pmf) 
    {
    	Map<String,PersistenceManager> map = (Map<String,PersistenceManager>)super.get();
    	PersistenceManager pm = null;
    	if (map != null)
    		pm = map.get(client);
    	
        if(pm == null || pm.isClosed()) 
        {
            pm = pmf.getPersistenceManager( ); 
            pm.setUserObject(client);
            if (map == null)
            	map = new HashMap<>();
            map.put(client, pm);
            set(map);
        }

        return pm;
    }
}
