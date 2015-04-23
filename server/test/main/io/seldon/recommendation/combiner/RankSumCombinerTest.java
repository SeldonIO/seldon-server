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

package io.seldon.recommendation.combiner;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import static io.seldon.recommendation.RecommendationPeer.*;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class RankSumCombinerTest {

    RecResultContext emptySet;
    RecResultContext notEnoughInEachSet;
    RecResultContext sufficientSet;
    RecResultContext plentySet;
    List<ItemRecommendationResultSet.ItemRecommendationResult> notEnoughInEachList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> sufficientList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> plentyList;
    ItemRecommendationResultSet.ItemRecommendationResult result1
            = new ItemRecommendationResultSet.ItemRecommendationResult(1L, 1.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result2
            = new ItemRecommendationResultSet.ItemRecommendationResult(2L, 2.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result3
            = new ItemRecommendationResultSet.ItemRecommendationResult(3L, 3.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result3Ranked
            = new ItemRecommendationResultSet.ItemRecommendationResult(3L, 2.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result2Ranked
            = new ItemRecommendationResultSet.ItemRecommendationResult(2L, 3.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result1Ranked
            = new ItemRecommendationResultSet.ItemRecommendationResult(1L, 1.0f);
    List<ItemRecommendationResultSet.ItemRecommendationResult> expectedRankSumList;

    @Before
    public void setup(){
        notEnoughInEachList = Arrays.asList(result1);
        sufficientList = Arrays.asList(result2, result1);
        plentyList = Arrays.asList(result3, result2, result1);

        emptySet = RecResultContext.EMPTY;

        notEnoughInEachSet = new RecResultContext(new ItemRecommendationResultSet(notEnoughInEachList, "notEnoguth"),"notEnoguth");

        sufficientSet = new RecResultContext(new ItemRecommendationResultSet(sufficientList, "sufficient"),"sufficient");
        plentySet = new RecResultContext(new ItemRecommendationResultSet(plentyList, "plenty"),"plenty");

        expectedRankSumList =  Arrays.asList(result2, result3, result1);
    }


    @Test
    public void isEnoughMethodShouldWorkCorrectly(){
        RankSumCombiner combiner = new RankSumCombiner(2);
        assertFalse(combiner.isEnoughResults(2, new ArrayList<RecResultContext>()));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(notEnoughInEachSet, emptySet )));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(notEnoughInEachSet, notEnoughInEachSet)));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(sufficientSet)));
        assertTrue(combiner.isEnoughResults(2,Arrays.asList(sufficientSet, sufficientSet)));
    }

    @Test
    public void shouldCombineResultsForRankSum(){
        RankSumCombiner combiner = new RankSumCombiner(2);
        RecResultContext result = combiner.combine(2, Arrays.asList(sufficientSet));
        assertEquals(sufficientSet.resultSet, result.resultSet);
        result = combiner.combine(2, Arrays.asList(notEnoughInEachSet, sufficientSet));
        assertEquals(sufficientSet.resultSet, result.resultSet);
        result = combiner.combine(2, Arrays.asList(sufficientSet, plentySet));
        assertEquals(new ItemRecommendationResultSet(Arrays.asList(result2Ranked, result3Ranked, result1Ranked), "plenty"), result.resultSet);

    }


}