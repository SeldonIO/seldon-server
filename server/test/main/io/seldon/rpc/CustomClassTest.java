package io.seldon.rpc;

import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import io.seldon.api.rpc.ClassificationReply;
import io.seldon.api.rpc.example.CustomPredictReply;

public class CustomClassTest {

	
	@Test
	public void testParseFromJSON() throws InvalidProtocolBufferException
	{
		String json = "{\"meta\":{\"modelName\":\"some-name\"},\"custom\":{\"@type\":\"type.googleapis.com/io.seldon.api.rpc.example.CustomPredictReply\",\"data\":\"some custom data\"}}";
		ClassificationReply.Builder builder = ClassificationReply.newBuilder();
		CustomPredictReply.Builder customBuilder = CustomPredictReply.newBuilder();
		TypeRegistry registry = TypeRegistry.newBuilder().add(customBuilder.getDescriptorForType()).build();
		JsonFormat.Parser jFormatter = JsonFormat.parser().usingTypeRegistry(registry);
		jFormatter.merge(json, builder);
		ClassificationReply reply = builder.build();
		System.out.println(reply);
	}
}
