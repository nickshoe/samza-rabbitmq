package io.github.nickshoe.samza.system.rabbitmq.descriptors;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.InputTransformer;
import org.apache.samza.system.descriptors.SystemDescriptor;

public class RabbitMQInputDescriptor<StreamMessageType> extends InputDescriptor<StreamMessageType, RabbitMQInputDescriptor<StreamMessageType>> {

	public RabbitMQInputDescriptor(String streamId, Serde<?> serde, SystemDescriptor<?> systemDescriptor, InputTransformer<?> transformer) {
		super(streamId, serde, systemDescriptor, transformer);
	}

}
