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

import io.seldon.api.resource.service.PersistenceProvider;
import io.seldon.general.ItemPeer;
import io.seldon.general.jdo.SqlItemPeer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JDOPersistenceProvider implements PersistenceProvider {
    
    @Autowired
    private JDOManager manager;


    public void setJdoManager(JDOManager manager){
        this.manager = manager;
    }


	@Override
	public ItemPeer getItemPersister(String clientName) {
		return new SqlItemPeer(manager.getJdoPersistenceManager(clientName));
	}

}
