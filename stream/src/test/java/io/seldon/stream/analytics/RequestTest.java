package io.seldon.stream.analytics;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Assert;

public class RequestTest {

	@Test
	public void testRecommendationPattern() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/users/22/recommendations\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/users/{userid}/recommendations", r.path);

	}
	
	@Test
	public void testActionPatterns() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/users/22/actions\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/users/{userid}/actions", r.path);

	}
	
	@Test
	public void testActionPatterns2() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/items/22/actions\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/items/{itemid}/actions", r.path);

	}

	@Test
	public void testActionPatterns3() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/users/22/actions/33\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/users/{userid}/actions/{itemid}", r.path);

	}

	@Test
	public void testActionPatterns4() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/items/22/actions/33\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/items/{itemid}/actions/{userid}", r.path);

	}

	@Test
	public void testActionPatterns5() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/actions/22\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/actions/{actionid}", r.path);

	}

	@Test
	public void testItemPatterns() throws JsonParseException, IOException
	{
		String json = "{\"consumer\":\"dailyrecord\",\"httpmethod\":\"GET\",\"path\":\"/items/22\",\"exectime\":\"34\",\"time\":123456789}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode jNode = mapper.readTree(parser);
	    Request r = new Request(jNode);
	    Assert.assertNotNull(r);
	    Assert.assertEquals("/items/{itemid}", r.path);

	}
	
	
}
