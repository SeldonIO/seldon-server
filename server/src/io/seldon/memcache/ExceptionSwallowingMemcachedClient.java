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

import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.GlobalConfigUpdateListener;
import io.seldon.api.state.ZkCuratorHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil Date: 18/11/14 Time: 14:06
 */
@Component
public class ExceptionSwallowingMemcachedClient implements GlobalConfigUpdateListener {

    private final String ZK_CONFIG_KEY_MEMCACHED_SERVERS = "memcached_servers";
    private final String ZK_CONFIG_KEY_MEMCACHED_SERVERS_FPATH = "/config/" + ZK_CONFIG_KEY_MEMCACHED_SERVERS;
    private static Logger logger = Logger.getLogger(ExceptionSwallowingMemcachedClient.class.getName());
    public static final int MEMCACHE_OP_TIMEOUT = 5000;
    private MemcachedClient memcachedClient = null;

    @Autowired
    public ExceptionSwallowingMemcachedClient(GlobalConfigHandler globalConfigHandler, ZkCuratorHandler zkCuratorHandler) throws Exception {
        logger.info("Initializing...");

        Stat stat = zkCuratorHandler.getCurator().checkExists().forPath(ZK_CONFIG_KEY_MEMCACHED_SERVERS_FPATH);
        if (stat != null) {
            byte[] bytes = zkCuratorHandler.getCurator().getData().forPath(ZK_CONFIG_KEY_MEMCACHED_SERVERS_FPATH);
            String servers = new String(bytes);
            memcachedClient = new MemcachedClient(new ConnectionFactoryBuilder(new DefaultConnectionFactory()).setOpTimeout(MEMCACHE_OP_TIMEOUT).build(),
                    AddrUtil.getAddresses(servers));
            logger.info(String.format("MemcachedClient initialized using %s[%s]", ZK_CONFIG_KEY_MEMCACHED_SERVERS, servers));
            
            MemCachePeer.initialise(servers);
        }

        if (memcachedClient == null) {
            throw new Exception("*Warning* Memcached NOT initialized!");
        }
        globalConfigHandler.addSubscriber(ZK_CONFIG_KEY_MEMCACHED_SERVERS, this);
    }

    /*
     * Expire in seconds
     */
    public OperationFuture<Boolean> set(String key, int expireSeconds, Object obj) {
        try {
            return memcachedClient.set(hashKey(key), expireSeconds, obj);
        } catch (Exception ex) {
            logger.warn("Memcache put expire exeption ", ex);
            return null;
        }
    }

    public Object get(String key) {
        Object myObj = null;
        Future<Object> f = memcachedClient.asyncGet(hashKey(key));
        try {
            myObj = f.get(MEMCACHE_OP_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.warn("Timeout exception in get ", e);
            f.cancel(false);
        } catch (InterruptedException e) {
            logger.error("Interrupted in get ", e);
            f.cancel(false);
        } catch (ExecutionException e) {
            logger.error("Execution exception in get ", e);
            f.cancel(false);
        }
        return myObj;
    }

    private static String hashKey(String key) {
        return SecurityHashPeer.md5digest(key);
    }

    @Override
    public void configUpdated(String configKey, String configValue) {
        // TODO memcached config updated in zookeeper
    }
}
