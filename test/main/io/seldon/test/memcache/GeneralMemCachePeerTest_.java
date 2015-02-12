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

package io.seldon.test.memcache;

import org.junit.Test;

import io.seldon.memcache.GeneralMemCachePeer;

public class GeneralMemCachePeerTest_ {

    @Test
    public void test_put() {
        GeneralMemCachePeer generalMemCachePeer = new GeneralMemCachePeer();
        
        String serverList = "localhost:11211";
        generalMemCachePeer.initialise(serverList);
        
        String key = "KEY_1";
        String value = "SOME_VALUE";
        
        { // Do put
            generalMemCachePeer.put(key, value);
        }
    }

    @Test
    public void test_get() {
        GeneralMemCachePeer generalMemCachePeer = new GeneralMemCachePeer();
        
        String serverList = "localhost:11211";
        generalMemCachePeer.initialise(serverList);
        
        String key = "KEY_1";
        String value = null;
        { // Do get
            value = (String)generalMemCachePeer.get(key);
        }
        
        System.out.println(value);
    }
    
    
    @Test
    public void test_put__waitForResult() {

        GeneralMemCachePeer generalMemCachePeer = new GeneralMemCachePeer();
        
        String serverList = "localhost:11211";
        generalMemCachePeer.initialise(serverList);
        
        String key = "KEY_1";
        String value = "SOME_VALUE";
        boolean result = false;
        { // Do put
            result = generalMemCachePeer.put_waitForResult(key, value);
        }
        System.out.println(result);
    }
    
    
}
