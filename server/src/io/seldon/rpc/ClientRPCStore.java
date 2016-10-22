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
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

@Component
public class ClientRPCStore implements PerClientExternalLocationListener  {
	
	private static Logger logger = Logger.getLogger(ClientRPCStore.class.getName());
    public static final String FILTER_NEW_RPC_PATTERN = "rpc";
    public final NewResourceNotifier notifier;
    
    ConcurrentHashMap<String,RPCConfigMap> services = new ConcurrentHashMap<String, ClientRPCStore.RPCConfigMap>();

    @Autowired
    public ClientRPCStore(NewResourceNotifier notifier) {
		logger.info("starting up");
		this.notifier = notifier;
		notifier.addListener(FILTER_NEW_RPC_PATTERN, this);
	}
    
    public RPCConfig getRPCConfig(String client,String rpcName)
    {
    	if (services.containsKey(client))
    	{
    		return services.get(client).nameMap.get(rpcName);
    	}
    	else
    		return null;
    }
    
    void add(String client,String rpcName,Class<?> requestClass,Class<?> responseClass)
    {
    	RPCConfig config = new RPCConfig();
    	config.requestClass = requestClass;
    	config.responseClass = responseClass;
    	if (!services.containsKey(client))
			services.putIfAbsent(client, new RPCConfigMap());
		services.get(client).nameMap.put(rpcName, config);
    }
    
    private JsonNode getJSONFromMethod(Method m,Message request) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, JsonParseException, IOException
    {
    	Message.Builder o2 = (Message.Builder) m.invoke(null);
    	TypeRegistry registry = TypeRegistry.newBuilder().add(o2.getDescriptorForType()).build();
    	
    	JsonFormat.Printer jPrinter = JsonFormat.printer();
    	String result = jPrinter.usingTypeRegistry(registry).print(request);
    	ObjectMapper mapper = new ObjectMapper();
    	JsonFactory factory = mapper.getFactory();
    	JsonParser parser = factory.createParser(result);
    	JsonNode jNode = mapper.readTree(parser);
    	if (jNode.has("custom") && jNode.get("custom").has("@type"))
    		((ObjectNode) jNode.get("custom")).remove("@type");
    	return jNode;
    }
    
    public JsonNode getJSONForRequest(String client,String rpcName,PredictRequest request)
    {
    	RPCConfig config = getRPCConfig(client, rpcName);
    	if (config != null)
    	{
    		try
    		{
    			Method m = config.requestClass.getMethod("newBuilder");
    			return getJSONFromMethod(m, request);
    		} catch (Exception e) {
    			logger.error("Failed to create JSON request for client "+client+" for rpc "+rpcName,e);
    			return null;
    		}
    	}
    	else
    	{
    		logger.warn("Failed to get RPC config for client "+client+" for rpc "+rpcName);
    		return null;
    	}
    }
    
    public JsonNode getJSONForReply(String client,String rpcName,PredictReply request)
    {
    	RPCConfig config = getRPCConfig(client, rpcName);
    	if (config != null)
    	{
    		try
    		{
    			Method m = config.responseClass.getMethod("newBuilder");
    			return getJSONFromMethod(m, request);
    		} catch (Exception e) {
    			logger.error("Failed to create JSON reply for client "+client+" for rpc "+rpcName,e);
    			return null;
    		}
    	}
    	else
    	{
    		logger.warn("Failed to get RPC config for client "+client+" for rpc "+rpcName);
    		return null;
    	}
    }
    
    private void createClientConfig(String client,String key,String data) 
    {
    	try
    	{
    		String rpcName = key.split("/")[1];
    	
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
    			this.add(client, rpcName, requestClass, responseClass);
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
			String parts[] = nodePattern.split("/");
			if (parts.length == 2)
			{
				this.createClientConfig(client, nodePattern, data);
			}
			else
			{
				logger.warn("Invalid node pattern "+nodePattern+" for client "+client);
			}
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
