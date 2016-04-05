/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.general;

import io.seldon.general.jdo.SqlItemPeer;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ItemStorageTest {

	@Test
	public void jsonItemScoreTest() throws JsonParseException, JsonMappingException, IOException
	{
		String json2= "[{\"item\":10357,\"score\":1817.0},{\"item\":10425,\"score\":1517.0},{\"item\":10308,\"score\":1448.0},{\"item\":9622,\"score\":1310.0},{\"item\":10423,\"score\":1214.0},{\"item\":9658,\"score\":1165.0},{\"item\":21069,\"score\":1056.0},{\"item\":9906,\"score\":1010.0},{\"item\":10427,\"score\":1008.0},{\"item\":10344,\"score\":1003.0},{\"item\":9882,\"score\":983.0},{\"item\":9640,\"score\":942.0},{\"item\":9676,\"score\":931.0},{\"item\":9900,\"score\":930.0},{\"item\":10306,\"score\":895.0},{\"item\":10276,\"score\":891.0},{\"item\":9656,\"score\":870.0},{\"item\":9896,\"score\":861.0},{\"item\":10430,\"score\":858.0},{\"item\":9855,\"score\":825.0},{\"item\":10442,\"score\":813.0},{\"item\":10436,\"score\":785.0},{\"item\":9661,\"score\":770.0},{\"item\":10435,\"score\":764.0},{\"item\":10277,\"score\":760.0},{\"item\":10366,\"score\":758.0},{\"item\":9757,\"score\":742.0},{\"item\":10367,\"score\":729.0}]";
		String json	= "[{\"item\":1,\"score\":1}]";
		final ObjectMapper mapper = new ObjectMapper();
		TypeReference t = new TypeReference<List<SqlItemPeer.ItemAndScore>>() {};
		List<SqlItemPeer.ItemAndScore> retrievedItems = mapper.readValue(json,t);
		
	}
}
