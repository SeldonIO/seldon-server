package io.seldon.rpc;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * RPC Channel Handler. Basic implementation that returns new channel for every call. 
 * @author clive
 *
 */
@Component
public class SimpleRPCChannelHandlerImpl implements RPCChannelHandler {

	ConcurrentHashMap<String,ManagedChannel> channels;
	
	@Override
	public ManagedChannel getChannel(String host,int port)
	{
		String key = host+":"+port;
		if (channels.containsKey(key))
			return channels.get(key);
		else
		{
			ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
					// Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
					// needing certificates.
					.usePlaintext(true)
					.build();
			channels.put(key,channel);
			return channel;
		}
	}
	
}
