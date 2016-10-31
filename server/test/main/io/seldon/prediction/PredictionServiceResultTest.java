package io.seldon.prediction;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import junit.framework.Assert;

public class PredictionServiceResultTest {

	
	@Test
	public void createFromJSONPredictionsOnly() throws JsonProcessingException, IOException
	{
		String json = "{\"predictions\":[{\"predictedClass\":\"cl1\",\"prediction\":1.0,\"confidence\":0.9}]}";
		ObjectMapper mapper = new ObjectMapper();
		ObjectReader reader = mapper.reader(PredictionServiceResult.class);
		PredictionServiceResult res = reader.readValue(json);
		Assert.assertNotNull(res);
		Assert.assertEquals(1.0, res.predictions.get(0).prediction);
	}
	
	@Test
	public void createFromJSONPreditionsAndMeta() throws JsonProcessingException, IOException
	{
		String json = "{\"meta\":{\"modelName\":\"amodel\"},\"predictions\":[{\"predictedClass\":\"cl1\",\"prediction\":1.0,\"confidence\":0.9}]}";
		ObjectMapper mapper = new ObjectMapper();
		ObjectReader reader = mapper.reader(PredictionServiceResult.class);
		PredictionServiceResult res = reader.readValue(json);
		Assert.assertNotNull(res);
		Assert.assertEquals(1.0, res.predictions.get(0).prediction);
		Assert.assertEquals("amodel", res.getMeta().modelName);
	}
	
	@Test
	public void createFromJSON() throws JsonProcessingException, IOException
	{
		String json = "{\"meta\":{\"modelName\":\"amodel\"},\"predictions\":[{\"predictedClass\":\"cl1\",\"prediction\":1.0,\"confidence\":0.9}],\"custom\":{\"somefield\":\"somevalue\"}}";
		ObjectMapper mapper = new ObjectMapper();
		ObjectReader reader = mapper.reader(PredictionServiceResult.class);
		PredictionServiceResult res = reader.readValue(json);
		Assert.assertNotNull(res);
		Assert.assertEquals(1.0, res.predictions.get(0).prediction);
		Assert.assertEquals("amodel", res.getMeta().modelName);
	}
}
