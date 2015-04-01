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
 * Created on 09-May-2006
 *
 */
package io.seldon.db.jdo;

import java.sql.SQLException;

import javax.jdo.PersistenceManager;


public abstract class Transaction {

    protected Object result = null;
    private PersistenceManager pm;
    public Transaction(PersistenceManager pm)
    {
    	this.pm = pm;
    }
    
    public PersistenceManager getPersistenceManager() { return pm; }
    public abstract void process() throws DatabaseException, SQLException;
    public Object getResult() { return result; }
    
    
}
