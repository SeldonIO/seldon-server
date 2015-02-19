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

import java.io.IOException;
import java.util.Properties;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.CASMutation;
import net.spy.memcached.CASMutator;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.apache.log4j.Logger;

public class GeneralMemCachePeer {

	private static Logger logger = Logger.getLogger(GeneralMemCachePeer.class);
	private static final int MAX_CAS_RETRIES = 100;

	private static MemcachedClient _client = null;

	public static String PROPNAME_SERVERLIST_MAIN = "io.seldon.memcached.servers";
	public static String PROPNAME_SERVERLIST_MGM_LIKES = "io.seldon.memcached.servers_mgm_likes";
	
	// set up connection pool once at class load
	public MemcachedClient initialise(Properties props, String prop_name_serverlist)
	{
		String serverList = props.getProperty(prop_name_serverlist);
        return initialise(serverList);
    }

    public MemcachedClient initialise(String serverList) {
        try
        {
            _client=new MemcachedClient(AddrUtil.getAddresses(serverList));
            logger.info("Created memcache connection for: "+serverList);
            return _client;
        }
        catch (IOException e)
        {
            logger.error("Can't create memcache connection ",e);
            return null;
        }
    }

	public void delete(String key)
	{
		if (_client != null)
		try
		{
			_client.delete(hashKey(key));
		}
		catch (Exception ex)
		{
			logger.warn("Memcache delete exeption ",ex);
		}
	}
	
	public void put(String key,Object obj)
	{
		if (_client != null)
		try
		{
			_client.set(hashKey(key), 0, obj);
		}
		catch (Exception ex)
		{
			logger.warn("Memcache put exeption ",ex);
		}
	}
	
	/*
	 *  Expire in seconds
	 */
	public void put(String key,Object obj,int expireSeconds)
	{
		if (_client != null)
			try
			{
				_client.set(hashKey(key), expireSeconds, obj);
			}
			catch (Exception ex)
			{
				logger.warn("Memcache put expire exeption ",ex);
			}
	}
	
	public boolean put_waitForResult(String key,Object obj) {
	    return put_waitForResult(key, obj, 0);
	}
	
	public boolean put_waitForResult(String key,Object obj,int expireSeconds) {
	    boolean retVal = false;
		if (_client != null)
			try
			{
				OperationFuture<Boolean> result = _client.set(hashKey(key), expireSeconds, obj);
				retVal = result.get();
			}
			catch (Exception ex)
			{
				logger.warn("Memcache put expire exeption ",ex);
			}
	    return retVal;
	}
	
	public Object get(String key)
	{
		if (_client != null)
		{
		try
		{
			Object res = _client.get(hashKey(key));
			/*if(res==null) { logger.info("Memcache: Requested="+key+" - found=NO"); }
			else { logger.info("Memcache: Requested="+key+" - found=YES"); }*/
			return res;
		}
		catch (Exception ex)
		{
			logger.warn("Memcache get exeption ",ex);
			return null;
		}
		}
		else
			return null;
	}
	
	public CASValue gets(String key)
	{
		if (_client != null)
		{
		try
		{
			return _client.gets(hashKey(key));
		}
		catch (Exception ex)
		{
			logger.warn("Memcache get exeption ",ex);
			return null;
		}
		}
		else
			return null;
	}
	
	public <T> T cas(String key,CASMutation<T> mutation,T value)
	{
		return cas(key,mutation,value,0);
	}
	
	/**
	 * Method to allow CAS 
	 * @param <T>
	 * @param key
	 * @param mutation
	 * @param value
	 * @return
	 */
	public <T> T cas(String key,CASMutation<T> mutation,T value,int expireSecs)
	{
		 if (_client != null)
		 {
			 Transcoder transcoder = new SerializingTranscoder();
			 // The mutator who'll do all the low-level stuff.
			 // Set number of retries to limit time taken..its not essential this succeeds
			 CASMutator<T> mutator = new CASMutator<>(_client, transcoder,MAX_CAS_RETRIES);

			 // This returns whatever value was successfully stored within the
			 // cache -- either the initial list as above, or a mutated existing
			 // one
			 try 
			 {
				 return mutator.cas(hashKey(key), value, expireSecs, mutation);
			 } 
			 catch (Exception e) 
			 {
				 logger.error("Failed up update hits in cache ",e);
				 return null;
			 }
		 }
		 else
			 return null;
	}
	
	private String hashKey(String key)
	{
		return SecurityHashPeer.md5digest(key);
	}
	
	
	public MemcachedClient getClient()
	{
		return _client;
	}
	
}
