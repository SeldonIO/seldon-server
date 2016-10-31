package io.seldon.rpc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;

import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;

import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.ClassificationReplyMeta;
import io.seldon.api.rpc.ClassificationRequest;
import io.seldon.api.rpc.ClassificationRequestMeta;
import io.seldon.api.rpc.example.CustomPredictReply;
import io.seldon.api.rpc.example.CustomPredictRequest;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.NewResourceNotifier;
import junit.framework.Assert;

public class ClientRPCStoreTest {
	
	private NewResourceNotifier mockNewResourceNotifier;
	
	@Before
	public void createMocks()
	{
		mockNewResourceNotifier = createMock(NewResourceNotifier.class);
	}
	
	@Test
	public void testGetPredictReplyFromJson() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		final String json = "{\"meta\":{\"modelName\":\"some-name\"},\"custom\":{\"@type\":\"type.googleapis.com/io.seldon.api.rpc.example.CustomPredictReply\",\"data\":\"some custom data\"}}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode actualObj = mapper.readTree(parser);
		store.add(client, null, CustomPredictReply.class);			
		ClassificationReply reply = store.getPredictReplyFromJson(client, actualObj);
		Assert.assertNotNull(reply);
		System.out.println(reply);
	}
	
	@Test
	public void testGetPredictReplyFromJsonWithNoType() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		final String json = "{\"meta\":{\"modelName\":\"some-name\"},\"custom\":{\"data\":\"some custom data\"}}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode actualObj = mapper.readTree(parser);
		store.add(client, null, CustomPredictReply.class);			
		ClassificationReply reply = store.getPredictReplyFromJson(client, actualObj);
		Assert.assertNotNull(reply);
		System.out.println(reply);
	}
	
	
	@Test
	public void testGetPredictRequestFromJson() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		final String json = "{\"meta\":{\"puid\":1234},\"data\":{\"@type\":\"type.googleapis.com/io.seldon.api.rpc.example.CustomPredictRequest\",\"data\":[1.2]}}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode actualObj = mapper.readTree(parser);
		store.add(client, CustomPredictRequest.class,null);			
		ClassificationRequest request = store.getPredictRequestFromJson(client, actualObj);
		Assert.assertNotNull(request);
		System.out.println(request);
	}
	
	@Test
	public void testGetPredictRequestFromJsonWithNoType() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		final String json = "{\"meta\":{\"puid\":1234},\"data\":{\"data\":[1.2]}}";
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json);
	    JsonNode actualObj = mapper.readTree(parser);
		store.add(client, CustomPredictRequest.class,null);			
		ClassificationRequest request = store.getPredictRequestFromJson(client, actualObj);
		Assert.assertNotNull(request);
		System.out.println(request);
	}
	@Test 
	public void testRequestToJSON() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		CustomPredictRequest customRequest =  CustomPredictRequest.newBuilder().addData(1.0f).build();
		store.add(client, customRequest.getClass(), null);
		Any anyMsg = Any.pack(customRequest);
		ClassificationRequestMeta meta = ClassificationRequestMeta.newBuilder().setPuid("1234").build();
		ClassificationRequest request = ClassificationRequest.newBuilder().setMeta(meta).setData(anyMsg).build();
		JsonNode json = store.getJSONForRequest(client, request);
		Assert.assertNotNull(json);
		System.out.println(json);
		ObjectMapper mapper = new ObjectMapper();
	    JsonFactory factory = mapper.getFactory();
	    JsonParser parser = factory.createParser(json.toString());
	    JsonNode actualObj = mapper.readTree(parser);
	    ClassificationRequest req = store.getPredictRequestFromJson(client, actualObj);
	    Assert.assertNotNull(req);
	}
	
	@Test 
	public void testMissingCustomRequesToJSON() throws JsonParseException, IOException
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		CustomPredictRequest customRequest =  CustomPredictRequest.newBuilder().addData(1.0f).build();
		store.add(client, customRequest.getClass(), null);
		ClassificationRequestMeta meta = ClassificationRequestMeta.newBuilder().setPuid("1234").build();
		ClassificationRequest request = ClassificationRequest.newBuilder().setMeta(meta).build();
		JsonNode json = store.getJSONForRequest(client,request);
		Assert.assertNotNull(json);
		System.out.println(json);
		
	}
	
	@Test 
	public void testResponseToJSON()
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		CustomPredictReply customResponse =  CustomPredictReply.newBuilder().setData("some value").build();
		store.add(client, null, customResponse.getClass());
		Any anyMsg = Any.pack(customResponse);
		ClassificationReplyMeta meta = ClassificationReplyMeta.newBuilder().setPuid("1234").build();
		ClassificationReply request = ClassificationReply.newBuilder().setMeta(meta).setCustom(anyMsg).build();
		JsonNode json = store.getJSONForReply(client, request);
		Assert.assertNotNull(json);
		System.out.println(json);
	}
	
	
	@Test 
	public void testMissingCustomResponse()
	{
		mockNewResourceNotifier.addListener((String) EasyMock.anyObject(), (PerClientExternalLocationListener) EasyMock.anyObject());
		EasyMock.expectLastCall().once();
		replay(mockNewResourceNotifier);
		final String client = "test";
		ClientRpcStore store = new ClientRpcStore(mockNewResourceNotifier);
		CustomPredictReply customResponse =  CustomPredictReply.newBuilder().setData("some value").build();
		store.add(client, null, customResponse.getClass());
		ClassificationReplyMeta meta = ClassificationReplyMeta.newBuilder().setPuid("1234").build();
		ClassificationReply request = ClassificationReply.newBuilder().setMeta(meta).build();
		JsonNode json = store.getJSONForReply(client,request);
		Assert.assertNotNull(json);
		System.out.println(json);
	}
	
}
