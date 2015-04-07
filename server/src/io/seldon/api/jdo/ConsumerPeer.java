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

package io.seldon.api.jdo;

import java.util.Collection;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.db.jdo.JDOFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class ConsumerPeer {

	@Autowired
	private JDOFactory jdoFactory;
	
	public Consumer findConsumer(String consumerKey) throws APIException {
		try {
			PersistenceManager pm =  jdoFactory.getPersistenceManager(Constants.API_DB);
			Query query = pm.newQuery(Consumer.class, "consumerKey == c");
			query.declareParameters("java.lang.String c");
			Collection<Consumer> c = (Collection<Consumer>) query.execute(consumerKey);
			if(c!=null && c.size()>0) {
				return c.iterator().next();
			}
			else return null;
		}
		catch(Exception e) {
			ApiLoggerServer.log(new ConsumerPeer(), e);
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
	}
}
