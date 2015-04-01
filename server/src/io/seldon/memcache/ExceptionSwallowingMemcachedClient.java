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

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author firemanphil
 *         Date: 18/11/14
 *         Time: 14:06
 */
@Component
public class ExceptionSwallowingMemcachedClient extends MemcachedClient {

    private static Logger logger = Logger.getLogger( ExceptionSwallowingMemcachedClient.class.getName() );

    @Autowired
    public ExceptionSwallowingMemcachedClient(@Value("${io.seldon.memcached.servers}") String servers) throws IOException {
        super(new ConnectionFactoryBuilder(new DefaultConnectionFactory()).setOpTimeout(1000).build(),AddrUtil.getAddresses(servers));
    }

    /*
	 *  Expire in seconds
	 */
    @Override
    public OperationFuture<Boolean> set(String key,int expireSeconds,Object obj)
    {
        try
        {
            return super.set(hashKey(key), expireSeconds, obj);
        }
        catch (Exception ex)
        {
            logger.warn("Memcache put expire exeption ",ex);
            return null;
        }
    }

    @Override
    public Object get(String key)
    {
    	Object myObj=null;
    	Future<Object> f=super.asyncGet(hashKey(key));
    	try 
    	{
    		myObj=f.get(500, TimeUnit.MILLISECONDS);
    	} catch(TimeoutException e) {
				logger.warn("Timeout exception in get ",e);
				f.cancel(false);
    	} catch (InterruptedException e) {
				logger.error("Interrupted in get ",e);
				f.cancel(false);
    	} catch (ExecutionException e) {
				logger.error("Execution exception in get ",e);
				f.cancel(false);
    	}
	    return myObj;
    }


    private static String hashKey(String key)
    {
        return SecurityHashPeer.md5digest(key);
    }
}
