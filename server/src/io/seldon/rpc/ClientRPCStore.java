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

import io.seldon.api.rpc.PredictReply;
import io.seldon.api.rpc.PredictRequest;
import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.NewResourceNotifier;

@Component
public class ClientRPCStore implements PerClientExternalLocationListener  {
	
	private static Logger logger = Logger.getLogger(ClientRPCStore.class.getName());
    public static final String FILTER_NEW_RPC_PATTERN = "rpc";
    public final NewResourceNotifier notifier;
    
    public static final String REQUEST_CUSTOM_DATA_FIELD = "data";
    public static final String REPLY_CUSTOM_DATA_FIELD = "custom";
    
    ConcurrentHashMap<String,RPCConfig> services = new ConcurrentHashMap<String, ClientRPCStore.RPCConfig>();

    @Autowired
    public ClientRPCStore(NewResourceNotifier notifier) {
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
    	if (jNode.has(fieldname) && jNode.get(fieldname).has("@type"))
    		((ObjectNode) jNode.get(fieldname)).remove("@type");
    	return jNode;
    }
    
    public JsonNode getJSONForRequest(String client,PredictRequest request)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			Method m = config.requestClass.getMethod("newBuilder");
    			return getJSONFromMethod(m, request, REQUEST_CUSTOM_DATA_FIELD);
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
    
    public JsonNode getJSONForReply(String client,PredictReply request)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			Method m = config.responseClass.getMethod("newBuilder");
    			return getJSONFromMethod(m, request, REPLY_CUSTOM_DATA_FIELD);
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
    
    public PredictReply getPredictReplyFromJson(String client,JsonNode json)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			PredictReply.Builder builder = PredictReply.newBuilder();
    			Method m = config.responseClass.getMethod("newBuilder");
    			Message.Builder o = (Message.Builder) m.invoke(null);
    			TypeRegistry registry = TypeRegistry.newBuilder().add(o.getDescriptorForType()).build();
    			JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
    			jFormatter.merge(json.toString(), builder);
    			PredictReply reply = builder.build();
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
    
    public PredictRequest getPredictRequestFromJson(String client,JsonNode json)
    {
    	RPCConfig config = services.get(client);
    	if (config != null)
    	{
    		try
    		{
    			PredictRequest.Builder builder = PredictRequest.newBuilder();
    			Method m = config.requestClass.getMethod("newBuilder");
    			Message.Builder o = (Message.Builder) m.invoke(null);
    			TypeRegistry registry = TypeRegistry.newBuilder().add(o.getDescriptorForType()).build();
    			JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
    			jFormatter.merge(json.toString(), builder);
    			PredictRequest request = builder.build();
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
    			Class<?> requestClass = Class.forName(config.requestClassName,true,cLoader);
    			Class<?> responseClass = Class.forName(config.responseClassName,true,cLoader);
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
