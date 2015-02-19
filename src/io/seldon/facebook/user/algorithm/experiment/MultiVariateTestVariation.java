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

package io.seldon.facebook.user.algorithm.experiment;

import io.seldon.general.MgmAction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author philipince
 *         Date: 10/10/2013
 *         Time: 08:53
 */
public class MultiVariateTestVariation<T> {

    private final String label;

    public T getData() {
        return data;
    }

    private final T data;
    private final ConcurrentMap<MgmAction.MgmActionType, AtomicInteger> events =
            new ConcurrentHashMap<>();

    public MultiVariateTestVariation(String label, T data) {
        this.label = label;
        this.data = data;
    }

    public String getLabel() {
        return label;
    }


    public void registerEvent(MgmAction action) {
        AtomicInteger value = events.get(action.getType());
        if (value==null){
            value = events.putIfAbsent(action.getType(), new AtomicInteger(1));
        }
        if(value!=null){
            value.incrementAndGet();
        }
    }

    public Map<MgmAction.MgmActionType, AtomicInteger> getEvents(){
        return new HashMap<>(events);
    }
}
