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
 * Created on 03-May-2006
 *
 */
package io.seldon.db.jdo;

import java.sql.SQLException;

import org.apache.log4j.Logger;

public class SQLErrorPeer {

    private static Logger logger = Logger.getLogger( SQLErrorPeer.class.getName() );

    public static final int SQL_ERROR = 0;
    public static final int SQL_DUPLICATE_KEY = 1;
    public static final int SQL_DEADLOCK = 2;
    public static final int SQL_LOCK_TIMEOUT = 3;
    public static final int SQL_NETWORK_PROBLEM = 4;


    public static int diagnoseSQLError(SQLException sqlEx)
    {
        logger.warn("SQL Exception sqlState=" + sqlEx.getSQLState() + " error=" + sqlEx.getErrorCode() + " message:" + sqlEx.getMessage());
        // Error number should be general across database providers but may not be of course in reality...
        if(sqlEx.getSQLState() != null && sqlEx.getSQLState().startsWith("23000"))
        {
            return SQL_DUPLICATE_KEY;
        }
        else if (sqlEx.getSQLState() != null && sqlEx.getSQLState().startsWith("40001"))
        {
        	return SQL_DEADLOCK;   
        }
        else if (sqlEx.getErrorCode() == 1205)
        {
            // MySQL specific lock wait timeout
            return SQL_LOCK_TIMEOUT;
        }
        else if (sqlEx.getSQLState() != null && sqlEx.getSQLState().startsWith("08S01"))
        {
            // Communication problem from database driver to db server
            return SQL_NETWORK_PROBLEM;
        }
        else
        	return SQL_ERROR;
    }
    
}
