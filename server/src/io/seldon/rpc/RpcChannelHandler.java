package io.seldon.rpc;

import io.grpc.ManagedChannel;

public interface RpcChannelHandler {
	public ManagedChannel getChannel(String host,int port);
}
