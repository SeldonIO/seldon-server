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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.seldon.general.MgmAction;
import org.apache.mahout.math.MurmurHash;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author philipince
 *         Date: 09/10/2013
 *         Time: 09:46
 */
public class MultiVariateTest<T> {

    private static final int HASH_SEED = 5795;
    private final String consumerName;

    protected final Map<Range, MultiVariateTestVariation<T>> rateRangeToVariation = new HashMap<Range, MultiVariateTestVariation<T>>();
    protected final BiMap<String, MultiVariateTestVariation<T>> labelToVariation = HashBiMap.create();
    private final UUID testId = UUID.randomUUID();

    private final Date startDate = new Date();

    public MultiVariateTest(String consumerName, Map<MultiVariateTestVariation<T>, BigDecimal> algToRate) {
        this.consumerName = consumerName;
        BigDecimal currentHighest = BigDecimal.ZERO;
        for (Map.Entry<MultiVariateTestVariation<T>, BigDecimal> entry : algToRate.entrySet()) {
            BigDecimal lower = currentHighest;
            BigDecimal higher = currentHighest.add(entry.getValue());
            currentHighest = higher;
            rateRangeToVariation.put(new Range(lower, higher, entry.getValue()), entry.getKey());
            labelToVariation.put(entry.getKey().getLabel(), entry.getKey());
        }
    }

    public String getConsumerName() {
        return consumerName;
    }

    public UUID getTestId() {
        return testId;
    }

    public String sampleVariations(String userId) {
        Integer hash = MurmurHash.hash(userId.getBytes(), HASH_SEED);
        int sample = Math.abs(hash % 100);
        BigDecimal sampleDec = BigDecimal.valueOf(sample).divide(BigDecimal.valueOf(100));
        for (Range range : rateRangeToVariation.keySet()) {
            if (range.contains(sampleDec)) {
                return labelToVariation.inverse().get(rateRangeToVariation.get(range));
            }
        }
        return null;
    }

    public T sample(String userId) {
        Integer hash = MurmurHash.hash(userId.getBytes(), HASH_SEED);
        int sample = Math.abs(hash % 100);
        BigDecimal sampleDec = BigDecimal.valueOf(sample).divide(BigDecimal.valueOf(100));
        for (Range range : rateRangeToVariation.keySet()) {
            if (range.contains(sampleDec)) {
                return rateRangeToVariation.get(range).getData();
            }
        }
        return null;
    }

    public List<MultiVariateTestResult> retrieveResults() {
        List<MultiVariateTestResult> results = new ArrayList<MultiVariateTestResult>();
        for (Map.Entry<Range, MultiVariateTestVariation<T>> variation : rateRangeToVariation.entrySet()) {
            Map<MgmAction.MgmActionType, AtomicInteger> events = variation.getValue().getEvents();
            String label = variation.getValue().getLabel();
            MultiVariateTestResult result = new MultiVariateTestResult(startDate, new Date(), consumerName,
                    label, events, variation.getKey().size);
            results.add(result);
        }
        return results;
    }

    private static class Range {
        private final BigDecimal lower;
        private final BigDecimal higher;
        private final BigDecimal size;

        private Range(BigDecimal lower, BigDecimal higher, BigDecimal size) {
            this.lower = lower;
            this.higher = higher;
            this.size = size;
        }

        public boolean contains(BigDecimal number) {
            return number.compareTo(higher) < 0 && number.compareTo(lower) >= 0;
        }
    }

    public void registerTestEvent(String variationLabel, MgmAction action) {
        labelToVariation.get(variationLabel).registerEvent(action);
    }

    public Date getStartDate() {
        return startDate;
    }

    public static void main(String args[]) {
        Integer hash = MurmurHash.hash("test4_139259509424974".getBytes(), HASH_SEED);
        int sample = Math.abs(hash % 100);
        BigDecimal sampleDec = BigDecimal.valueOf(sample).divide(BigDecimal.valueOf(100));
        System.out.println(sampleDec);
        System.out.println(sample);
    }
}
