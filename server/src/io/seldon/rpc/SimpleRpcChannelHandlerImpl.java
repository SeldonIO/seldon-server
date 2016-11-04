package io.seldon.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.PredictionAlgorithmStore;

/**
 * RPC Channel Handler. Basic implementation that create a new Channel for each client until new zookeeper configuration appears which
 * might suggest a new service has been created and thus we should refresh the channel. This is required as DNS changes will be the channel might
 * keep failing and not get refreshed in present gRPC setup. See https://github.com/grpc/grpc-java/issues/1463
 * @author clive
 *
 */
@Component
public class SimpleRpcChannelHandlerImpl implements RpcChannelHandler,ClientConfigUpdateListener{

	private static Logger logger = Logger.getLogger(SimpleRpcChannelHandlerImpl.class.getName());
	ConcurrentHashMap<String,ConcurrentHashMap<String,ManagedChannel>> channels = new ConcurrentHashMap<String, ConcurrentHashMap<String,ManagedChannel>>();
	
	 private final ClientConfigHandler configHandler;
	 
	 @Autowired
	 public SimpleRpcChannelHandlerImpl(ClientConfigHandler configHandler)
	 {	
		 this.configHandler = configHandler;
	 }
	 
	 @PostConstruct
	 private void init(){
		 logger.info("Initializing...");
		 configHandler.addListener(this);
	 }
	
	private String getKey(String host,int port)
	{
		return host+":"+port;
	}
	
	private ManagedChannel addChannel(String client,String host,int port)
	{
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
				// Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
				// needing certificates.
				.usePlaintext(true)
				.build();
		ConcurrentHashMap<String,ManagedChannel> clientChannels = channels.putIfAbsent(client, new ConcurrentHashMap<String,ManagedChannel>());
		clientChannels.put(getKey(host,port), channel);
		channels.put(client, clientChannels);
		return channel;
	}
	
	private void clearChannels(String client)
	{
		Map<String,ManagedChannel> currentChannels = channels.put(client, new ConcurrentHashMap<String,ManagedChannel>());
		if (currentChannels != null)
			for (ManagedChannel ch : currentChannels.values())
			{
				ch.shutdown();
			}	
	}
	
	@Override
	public ManagedChannel getChannel(String client,String host,int port)
	{
		String key = host+":"+port;
		if (channels.containsKey(client) && channels.get(client).containsKey(key))
		{
			return channels.get(client).get(key);
		}
		else
		{
			return addChannel(client, host, port);
		}
	}


	@Override
	public void configUpdated(String client, String configKey, String configValue) 
	{
		if (configKey.equals(PredictionAlgorithmStore.ALG_KEY))
		{
			logger.info("Clearing existing channels for client "+client);
			clearChannels(client);
		}

	}


	@Override
	public void configRemoved(String client, String configKey) {
		if (configKey.equals(PredictionAlgorithmStore.ALG_KEY))
		{
			logger.info("Clearing existing channels for client "+client);
			clearChannels(client);
		}
	}

	
}
