package io.github.nickshoe.samza.system.rabbitmq.descriptors;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;

/**
 * This is class is heavily inspired from Samza's official KafkaOutputDescriptor class
 *
 * @param <StreamMessageType>
 */
public class RabbitMQOutputDescriptor<StreamMessageType> extends OutputDescriptor<StreamMessageType, RabbitMQOutputDescriptor<StreamMessageType>> {

	public RabbitMQOutputDescriptor(String streamId, Serde<?> serde, SystemDescriptor<?> systemDescriptor) {
		super(streamId, serde, systemDescriptor);
	}

}
