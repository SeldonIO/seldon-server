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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;

import org.apache.log4j.Logger;
import org.datanucleus.api.jdo.JDOTransaction;



/**
 * Handle Transactions in JDO (for mysql)
 * <ul>
 *  <li> Ensures a Read-Only transaction is used whne not inside Transction class. This allows mysql ReplicationDriver
 *  to send these commands to Slave mysql instances. Inside a Transaction commands will be sent to master for 
 *  writing.
 *  <li> Ensures dead-lock exceptions are caught and the transaction tried a again a set number of times before failing.
 *  <li> Allows nested transactions
 *  <li> Throws warnings if a write is done inside a read transaction
 * 
 * </ul>
 * @author rummble
 *
 */
public class TransactionPeer {
    private static Logger logger = Logger.getLogger( TransactionPeer.class.getName() );

    private static final int DEADLOCK_RETRIES = 5; // taken from mysql connector/j example - dunno if sensible
    
    //private static JDOLifecycleListener JDODirtyListener = new JDOLifecycleListener(); 
    private static NestedTransactionCounter nesting = new NestedTransactionCounter();
    
    public static void resetNestingCounter()
    {
    	nesting.set(null);
    }
    
    public static void runTransaction(Transaction t) throws DatabaseException
    {
        runTransaction(new HashSet(),t);  
    }
   
    
 
   
    public static void runTransaction(Collection members,Transaction t) throws DatabaseException
    {
        boolean success = false;
        boolean topLevelTransaction = nesting.startTransaction() == 1;
        try
        {
        	for (int tries=0;!success && tries<DEADLOCK_RETRIES;tries++)
            { 
                try
                {
                	if (topLevelTransaction)
                	{
                		PersistenceManager pm = t.getPersistenceManager();
                		closeReadOnlyTransaction(pm);
                		if (members.isEmpty())
                			TransactionPeer.startTransaction(t.getPersistenceManager());
                		//else
                		//	TransactionPeer.startTransaction(members);
                	}
                	else
                	{
                		// add locks for members. Its ok if these have been done already.
                		//if (!members.isEmpty())
                		//{
                		//	 PersistenceManager pm = JDOFactory.getNonTransactionalPersistenceManager();
                		//	 lockMembers(pm,members);
                		//}
                	}
                    t.process();
                    if (topLevelTransaction)
                    	TransactionPeer.commitTransaction(t.getPersistenceManager());
                    success = true;
                }
                catch (JDODataStoreException ex)
                {
                    //logger.warn("Caught jdo datastore exception",ex);
                    Throwable[] nested = ex.getNestedExceptions ();
                    for (int i = 0; nested != null && i < nested.length; i++)
                    {
                    	SQLException sqlEx = null;
                        if (nested[i] instanceof SQLException)
                        	sqlEx = (SQLException) nested[i];
                        if (sqlEx != null)
                        {
                            switch(SQLErrorPeer.diagnoseSQLError(sqlEx))
                            {
                            case SQLErrorPeer.SQL_DUPLICATE_KEY:
                                throw new DatabaseException(SQLErrorPeer.SQL_DUPLICATE_KEY);
                            case SQLErrorPeer.SQL_DEADLOCK:
                                logger.warn("Caught deadlock exception on attempt: " + tries);
                             break;
                            case SQLErrorPeer.SQL_LOCK_TIMEOUT:
                                 logger.warn("Caught lock wait timeout on attempt: " + tries);
                             break;
                            case SQLErrorPeer.SQL_NETWORK_PROBLEM:
                                 logger.warn("Caught comms error on attempt: " + tries);
                             break;
                            default:
                            {
                            	logger.error("Caught unknown JDO data store exception",sqlEx);
                                throw new DatabaseException(SQLErrorPeer.SQL_ERROR,ex);
                            }
                            }
                        }
                        else
                        	logger.error("Unknown nested exception in JDO exception, nested["+i+"]",nested[i]);
                    }
                }
                catch (JDOException ex)
                {
                    logger.error("Caught unhandled JDO exception",ex);
                    throw new DatabaseException(SQLErrorPeer.SQL_ERROR,ex);
                }
                catch (SQLException ex)
                {
                    switch(SQLErrorPeer.diagnoseSQLError(ex))
                    {
                    case SQLErrorPeer.SQL_DUPLICATE_KEY:
                        throw new DatabaseException(SQLErrorPeer.SQL_DUPLICATE_KEY);
                    case SQLErrorPeer.SQL_DEADLOCK:
                        logger.warn("Caught deadlock exception on attempt: " + tries);
                     break;
                    case SQLErrorPeer.SQL_LOCK_TIMEOUT:
                         logger.warn("Caught lock wait timeout on attempt: " + tries);
                     break;
                    case SQLErrorPeer.SQL_NETWORK_PROBLEM:
                         logger.warn("Caught comms error on attempt: " + tries);
                     break;
                    default:
                    {
                    	logger.error("Caught unknown database error:",ex);
                        throw new DatabaseException(SQLErrorPeer.SQL_ERROR,ex);
                    }
                    }
                }
                catch (NestedTransactionException ex)
                {
                	if (!topLevelTransaction)
                		throw new NestedTransactionException();
                }
                catch (DatabaseException ex)
                {
                	logger.info("Caught playtxt exception ",ex);
                	throw ex;
                }
                catch (Exception ex)
                {
                	logger.error("Caught exception during transaction:",ex);
                	throw new DatabaseException(-1,ex);
                	
                }
//                catch (PlaytxtException ex)
//                {
//                   logger.info("Caught playtxt exception ",ex);
//                    //PersistenceManager pm = JDOFactory.getNonTransactionalPersistenceManager();
//                    //pm.currentTransaction().rollback();
//                }
                finally
                {
                	if (topLevelTransaction)
                	{
                        rollbackIfActive(t.getPersistenceManager());
                        PersistenceManager pm = t.getPersistenceManager();
                        startReadOnlyTransaction(pm);
                	}
                	else
                	{
                		if (!success)
                		{
                			logger.warn("Nested Transaction failed");
                			throw new NestedTransactionException();
                		}
                	}
                        
                } 
            }
            if (!success)
            {
                final String message = "Failed to carry out transaction";
                logger.error(message, new Exception(message));
                throw new DatabaseException(-1);
            }
        }
        finally
        {
        	nesting.endTransaction();
        }
    }

    public static void startReadOnlyTransaction(PersistenceManager pm)
    {
        if (!pm.currentTransaction().isActive())
        {
            // Check to ensure we never write in a readOnly transaction.
            // as should never be called shouldn't use up much extra CPU cycles if any
            //pm.addInstanceLifecycleListener(JDODirtyListener,(Class[]) null);

           //((UserTransaction)pm.currentTransaction()).setUseUpdateLock(false);
            JDOTransaction tx = (JDOTransaction)pm.currentTransaction();
        	//tx.setOption("transaction.serializeReadObjects", false);
            tx.setOption("datanucleus.SerializeRead", false);
            pm.currentTransaction().begin();
            try
            {
                setMySQLReadOnly(pm,true);
            }
            catch (SQLException ex)
            {
                logger.error("Caught unexpected SQLException trying to set ReadOnly true", ex);
                SQLErrorPeer.diagnoseSQLError(ex);
                // What to do now? Need to handle the infamous "communications link error" here?
            }
        }
    }
    
    public static void closeReadOnlyTransaction(PersistenceManager pm) throws SQLException
    {
        if (pm.currentTransaction().isActive())
        {
            if (!isMySQLReadOnly(pm))
                logger.warn("We are not in a read-only transaction");
            // Check to ensure we never write in a readOnly transaction.
            /*
            if (JDODirtyListener.isDirty())
            {
                final String message = "Readonly transaction is dirty";
                logger.error(message, new Exception(message));
                JDODirtyListener.clear();
                //throw new PlaytxtFatalException(Errors.JDO_WRITE_IN_READONLY_TX);
            }
            pm.removeInstanceLifecycleListener(JDODirtyListener);
            */
            pm.currentTransaction().rollback();
        }
    }
    
    private static boolean isMySQLReadOnly(PersistenceManager pm) throws SQLException
    {
        JDOConnection jdoconn = pm.getDataStoreConnection();
        try
        {
            Connection conn = (Connection) jdoconn.getNativeConnection();
            //com.mysql.jdbc.ReplicationConnection mySqlConn = (com.mysql.jdbc.ReplicationConnection) conn;  
            Boolean val = conn.isReadOnly();
            return val;
        }
        finally
        {
            jdoconn.close();
        }

    }
    
    private static void setMySQLReadOnly(PersistenceManager pm,boolean readOnly) throws SQLException
    {
        JDOConnection jdoconn = pm.getDataStoreConnection();
        try
        {
            Connection conn = (Connection) jdoconn.getNativeConnection();
            //com.mysql.jdbc.ReplicationConnection mySqlConn = (com.mysql.jdbc.ReplicationConnection) conn;  
            conn.setReadOnly(readOnly);
            String catalog =  ((org.datanucleus.api.jdo.JDOPersistenceManagerFactory) pm.getPersistenceManagerFactory()).getCatalog();
            conn.setCatalog(catalog);
        }
        finally
        {
            jdoconn.close();
        }
    }

   

    private static void startTransaction(PersistenceManager pm) throws SQLException
    {
        //((UserTransaction)pm.currentTransaction()).setUseUpdateLock(true);
        JDOTransaction tx = (JDOTransaction)pm.currentTransaction();
    	//tx.setOption("transaction.serializeReadObjects", true);
        tx.setOption("datanucleus.SerializeRead", true);
        pm.currentTransaction().begin();
        setMySQLReadOnly(pm,false);
    }

   

  
    private static void commitTransaction(PersistenceManager pm) throws SQLException
    {
        pm.currentTransaction().commit();
    }

    public static void rollbackIfActive(PersistenceManager pm)
    {
        if (pm.currentTransaction().isActive())
        {
        	Throwable t = new Throwable();
        	t.fillInStackTrace();
            logger.error("Rolling back a transaction that was still active",t);
            try
            {
                pm.currentTransaction().rollback();
            }
            catch (Exception ex)
            {
                logger.error("Unexpected exception on roll back",ex);
            }
        }
    }
    
}
