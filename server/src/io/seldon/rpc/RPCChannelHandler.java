package io.seldon.rpc;

import io.grpc.ManagedChannel;

public interface RPCChannelHandler {
	public ManagedChannel getChannel(String host,int port);
}
