package io.seldon.rpc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import io.seldon.api.resource.service.business.PredictionBusinessServiceImpl;
import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.ClassificationRequest;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.NewResourceNotifier;


@Component
public class ClientRpcStore implements PerClientExternalLocationListener  {
	
	private static Logger logger = Logger.getLogger(ClientRpcStore.class.getName());
    public static final String FILTER_NEW_RPC_PATTERN = "rpc";
    public final NewResourceNotifier notifier;
    
     
    ConcurrentHashMap<String,RPCConfig> services = new ConcurrentHashMap<String, ClientRpcStore.RPCConfig>();

    @Autowired
    public ClientRpcStore(NewResourceNotifier notifier) {
		logger.info("starting up");
		this.notifier = notifier;
		notifier.addListener(FILTER_NEW_RPC_PATTERN, this);
	}
    
    public RPCConfig getRPCConfig(String client)
    {
    	if (services.containsKey(client))
    	{
    		return services.get(client);
    	}
    	else
    		return null;
    }
    
    void add(String client,Class<?> requestClass,Class<?> responseClass)
    {
    	RPCConfig config = new RPCConfig();
    	config.requestClass = requestClass;
    	config.responseClass = responseClass;
    	services.put(client, config);
    }
    
    private JsonNode getJSONFromMethod(Method m,Message msg,String fieldname) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, JsonParseException, IOException
    {
    	Message.Builder o2 = (Message.Builder) m.invoke(null);
    	TypeRegistry registry = TypeRegistry.newBuilder().add(o2.getDescriptorForType()).build();
    	
    	JsonFormat.Printer jPrinter = JsonFormat.printer();
    	String result = jPrinter.usingTypeRegistry(registry).print(msg);
    	ObjectMapper mapper = new ObjectMapper();
    	JsonFactory factory = mapper.getFactory();
    	JsonParser parser = factory.createParser(result);
    	JsonNode jNode = mapper.readTree(parser);
    	System.out.println(jNode);
    	//if (jNode.has(fieldname) && jNode.get(fieldname).has("@type"))
    	//	((ObjectNode) jNode.get(fieldname)).remove("@type");
    	return jNode;
    }

    private JsonNode getJSON(Message msg,String fieldname) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, JsonParseException, IOException
    {
    	JsonFormat.Printer jPrinter = JsonFormat.printer();
    	String result = jPrinter.print(msg);
    	ObjectMapper mapper = new ObjectMapper();
    	JsonFactory factory = mapper.getFactory();
    	JsonParser parser = factory.createParser(result);
    	JsonNode jNode = mapper.readTree(parser);
    	System.out.println(jNode);
    	return jNode;
    }
    
    public JsonNode getJSONForRequest(String client,ClassificationRequest request)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			if (config.requestClass != null)
    			{
    				Method m = config.requestClass.getMethod("newBuilder");
    				return getJSONFromMethod(m, request, PredictionBusinessServiceImpl.REQUEST_CUSTOM_DATA_FIELD);
    			}
    			else
    				return getJSON(request, PredictionBusinessServiceImpl.REQUEST_CUSTOM_DATA_FIELD);
    		} catch (Exception e) {
    			logger.error("Failed to create JSON request for client "+client,e);
    			return null;
    		}
    	}
    	else
    	{
    		logger.warn("Failed to get RPC config for client "+client);
    		return null;
    	}
    }
    
    public JsonNode getJSONForReply(String client,ClassificationReply request)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			if (config.responseClass != null)
    			{
    				Method m = config.responseClass.getMethod("newBuilder");
    				return getJSONFromMethod(m, request, PredictionBusinessServiceImpl.REPLY_CUSTOM_DATA_FIELD);
    			}
    			else
    				return getJSON(request, PredictionBusinessServiceImpl.REPLY_CUSTOM_DATA_FIELD);
    		} catch (Exception e) {
    			logger.error("Failed to create JSON reply for client "+client,e);
    			return null;
    		}
    	}
    	else
    	{
    		logger.warn("Failed to get RPC config for client "+client);
    		return null;
    	}
    }
    
    public ClassificationReply getPredictReplyFromJson(String client,JsonNode json)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			TypeRegistry registry = null;
    			if (config.responseClass != null && json.has(PredictionBusinessServiceImpl.REPLY_CUSTOM_DATA_FIELD))
    			{
    				if (!json.get(PredictionBusinessServiceImpl.REPLY_CUSTOM_DATA_FIELD).has("@type"))
    					((ObjectNode) json.get(PredictionBusinessServiceImpl.REPLY_CUSTOM_DATA_FIELD)).put("@type", "type.googleapis.com/" + config.responseClass.getName());
    				Method m = config.responseClass.getMethod("newBuilder");
    				Message.Builder o = (Message.Builder) m.invoke(null);
    				registry = TypeRegistry.newBuilder().add(o.getDescriptorForType()).build();
    			}
    			ClassificationReply.Builder builder = ClassificationReply.newBuilder();
    			JsonFormat.Parser jFormatter = JsonFormat.parser();
    			if (registry != null)
    				jFormatter = jFormatter.usingTypeRegistry(registry);
    			jFormatter.merge(json.toString(), builder);
    			ClassificationReply reply = builder.build();
    			return reply;
    		} catch (Exception e) {
    			logger.error("Failed to convert json "+json.toString()+" to PredictReply",e);
    			return null;
			}
    	}
		else
    	{
    		logger.warn("Failed to get RPC config for client "+client);
    		return null;
    	}
    }
    
    public ClassificationRequest getPredictRequestFromJson(String client,JsonNode json)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			TypeRegistry registry = null;
    			if (config.requestClass != null && json.has(PredictionBusinessServiceImpl.REQUEST_CUSTOM_DATA_FIELD))
    			{
    				if (!json.get(PredictionBusinessServiceImpl.REQUEST_CUSTOM_DATA_FIELD).has("@type"))
    					((ObjectNode) json.get(PredictionBusinessServiceImpl.REQUEST_CUSTOM_DATA_FIELD)).put("@type", "type.googleapis.com/" + config.requestClass.getName());
    				System.out.println(json);
    				Method m = config.requestClass.getMethod("newBuilder");
    				Message.Builder o = (Message.Builder) m.invoke(null);
    				registry = TypeRegistry.newBuilder().add(o.getDescriptorForType()).build();
    			}
				ClassificationRequest.Builder builder = ClassificationRequest.newBuilder();
    			JsonFormat.Parser jFormatter = JsonFormat.parser();
    			if (registry != null)
    				jFormatter = jFormatter.usingTypeRegistry(registry);
    			jFormatter.merge(json.toString(), builder);
    			ClassificationRequest request = builder.build();
    			return request;
    		} catch (Exception e) {
    			logger.error("Failed to convert json "+json.toString()+" to PredictRequest",e);
    			return null;
			}
    	}
		else
    	{
    		logger.warn("Failed to get RPC config for client "+client);
    		return null;
    	}
    }
    
    private void createClientConfig(String client,String key,String data) 
    {
    	try
    	{
    		ObjectMapper mapper = new ObjectMapper();
    		RPCZkConfig config = mapper.readValue(data, RPCZkConfig.class);

    		File f = new File(config.jarUrl);
    		try
    		{
    			URL myURL = f.toURI().toURL();
    			URL[] urls = {myURL};
    			URLClassLoader cLoader = new URLClassLoader (urls, this.getClass().getClassLoader());
    			Class<?> requestClass = null;
    			Class<?> responseClass = null;
    			if (org.apache.commons.lang.StringUtils.isNotEmpty(config.requestClassName))
    				requestClass = Class.forName(config.requestClassName,true,cLoader);
    			if (org.apache.commons.lang.StringUtils.isNotEmpty(config.responseClassName))
    				responseClass = Class.forName(config.responseClassName,true,cLoader);
    			this.add(client, requestClass, responseClass);
    		} catch (MalformedURLException e) 
    		{
    			logger.error("Bad url "+config.jarUrl,e);
			} catch (ClassNotFoundException e) {
				logger.error("Failed to load class ",e);
			}
    		finally{}
    	} catch (JsonParseException e1) {
    		logger.error("Failed to parse json "+data,e1);
		} catch (JsonMappingException e1) {
    		logger.error("Failed to parse json "+data,e1);
		} catch (IOException e1) {
    		logger.error("Failed to parse json "+data,e1);
		}
    	finally{}
    }
    
	

	@Override
	public void newClientLocation(String client, String data, String nodePattern) 
	{
		logger.info("New client location "+client+" at "+data+" node pattern "+nodePattern);
		if (org.apache.commons.lang.StringUtils.isNotEmpty(data))
		{
			this.createClientConfig(client, nodePattern, data);
		}
		else
		{
			logger.warn("Ignoring as no data provided "+nodePattern+" for client "+client);
		}
	}

	@Override
	public void clientLocationDeleted(String client, String nodePattern) 
	{
		logger.info("Client location deleted "+client+" at "+nodePattern);
	}
	
	public static class RPCConfigMap
	{
		public ConcurrentHashMap<String, RPCConfig> nameMap;

		public RPCConfigMap()
		{
			this.nameMap = new ConcurrentHashMap<>();
		}
		
	}

	public static class RPCConfig {
		public Class<?> requestClass;
		public Class<?> responseClass;
	}
	
	public static class RPCZkConfig {
		public String jarUrl;
		public String requestClassName;
		public String responseClassName;
		
	}
}
