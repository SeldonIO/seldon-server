package io.seldon.rpc;

import io.grpc.ManagedChannel;

public interface RpcChannelHandler {
	public ManagedChannel getChannel(String client,String host,int port);
}
