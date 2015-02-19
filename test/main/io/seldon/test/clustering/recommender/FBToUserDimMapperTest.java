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

package io.seldon.test.clustering.recommender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.clustering.recommender.UserCluster;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.clustering.recommender.FBToUserDimMapper;

public class FBToUserDimMapperTest {

	@Test
	public void test()
	{
		Map<String,Set<Integer>> map = new HashMap<>();
		final String filmCat = "Film";
		final String healthCat = "Health and beauty";
		final int dim1 = 1;
		final int dim2 = 2;
		Set<Integer> dims = new HashSet<>();
		dims.add(dim1);
		dims.add(dim2);
		map.put(filmCat, dims);
		FBToUserDimMapper mapper = new FBToUserDimMapper(map);
		
		List<String> userCategories = new ArrayList<>();
		userCategories.add(filmCat);
		userCategories.add(healthCat);
		
		List<UserCluster> res = mapper.suggestClusters(1, userCategories);
		Assert.assertEquals(2, res.size());
		for (UserCluster c : res)
		{
			Assert.assertTrue(c.getCluster() == dim1 || c.getCluster() == dim2);
		}
	}

}
