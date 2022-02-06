package io.github.nickshoe.samza.system.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class RabbitMQSystemProducer implements SystemProducer {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQSystemProducer.class);

	private final String systemName;
	private final Connection connection;
	private final Channel channel;

	public RabbitMQSystemProducer(String systemName, Connection connection, Channel channel) {
		super();
		this.systemName = systemName;
		this.connection = connection;
		this.channel = channel;
	}

	@Override
	public void start() {}

	@Override
	public void stop() {
		logger.info("Stopping producer for system: " + this.systemName);

		try {
			if (this.channel != null) {
				this.channel.close();
			}

			if (this.connection != null) {
				this.connection.close();
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while closing producer for system: " + systemName, e);
		}
	}

	@Override
	public void register(String source) {}

	@Override
	public void send(String source, OutgoingMessageEnvelope envelope) {
		logger.trace("Enqueuing message: {}, {}.", source, envelope);

	    String queueName = envelope.getSystemStream().getStream();
	    if (queueName == null || queueName.isEmpty()) {
	      throw new IllegalArgumentException("Invalid system stream: " + envelope.getSystemStream());
	    }

	    try {
	    	// TODO ser/des based on config
			String messageId = (String) envelope.getKey();
			byte[] messageBody = ((String) envelope.getMessage()).getBytes(StandardCharsets.UTF_8);
			
			AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().messageId(messageId).build();
			
			// TODO: should the exchange be topic-like? (otherwise round robin is being used...) - make it configurable?
			this.channel.basicPublish("", queueName, properties, messageBody);
		} catch (IOException e) {
			logger.error("Could not publish message to queue {}: {}", queueName, e.getMessage());
			
			throw new SystemProducerException(String.format("Failed to send message for Source: %s on System:%s Topic:%s", source, systemName, queueName), e);
		}
	}

	@Override
	public void flush(String source) {}

}
