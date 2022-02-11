package io.github.nickshoe.samza.system.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;

/**
 * This class is heavily inspired to Samza's official KafkaConsumerProxy class
 *
 * @param <K>
 * @param <V>
 */
public class RabbitMQConsumerProxy<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQSystemConsumer.class);

	private final Thread consumerProxyThread;
	private final Channel channel;
	private final RabbitMQSystemConsumer<K, V> rabbitMQSystemConsumer;
	private final RabbitMQSystemConsumer<K, V>.RabbitMQConsumerMessageSink sink;
	private final String systemName;

	private String queueName;
	private SystemStreamPartition ssp;

	private volatile boolean isRunning = false;
	private volatile Throwable failureCause = null;
	private final CountDownLatch consumerThreadStartLatch = new CountDownLatch(1);

	public RabbitMQConsumerProxy(
		RabbitMQSystemConsumer<K, V> systemConsumer,
		Channel channel,
		String systemName, 
		RabbitMQSystemConsumer<K, V>.RabbitMQConsumerMessageSink messageSink) {

		this.rabbitMQSystemConsumer = systemConsumer;
		this.sink = messageSink;

		this.channel = channel;
		this.systemName = systemName;

		this.consumerProxyThread = new Thread(createProxyThreadRunnable());
		this.consumerProxyThread.setDaemon(true);
		this.consumerProxyThread.setName("Samza RabbitMQConsumerProxy " + consumerProxyThread.getName() + " - " + systemName);

		logger.info("Creating RabbitMQConsumerProxy with systeName={}", systemName);
	}

	public boolean isRunning() {
		return isRunning;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	/**
	 * Set the system stream partition to be populated with RabbitMQ consumed messages. 
	 * Must be called before {@link RabbitMQConsumerProxy#start} is called.
	 * 
	 * @param ssp        - SystemStreamPartition to add
	 */
	public void setSystemStreamPartition(SystemStreamPartition ssp) {
		logger.info(String.format("Setting system stream partition %s to be populated from consumer %s", ssp, this));

		String queueName = ssp.getStream();

		this.queueName = queueName;
		this.ssp = ssp;
	}

	public void start() {
		if (!consumerProxyThread.isAlive()) {
			logger.info("Starting RabbitMQConsumerProxy thread for " + this.toString());

			consumerProxyThread.start();

			// we need to wait until the thread starts
			while (!isRunning && failureCause == null) {
				try {
					consumerThreadStartLatch.await(3000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					logger.info("Ignoring InterruptedException while waiting for consumer thread to start.", e);
				}
			}
		} else {
			logger.warn("Tried to start an already started RabbitMQConsumerProxy (%s). Ignoring.", this.toString());
		}
	}

	/**
	 * Stop this RabbitMQConsumerProxy and wait for at most {@code timeoutMs}.
	 * 
	 * @param timeoutMs maximum time to wait to stop this RabbitMQConsumerProxy
	 */
	public void stop(long timeoutMs) {
		logger.info("Shutting down RabbitMQConsumerProxy thread {} for {}", consumerProxyThread.getName(), this);

		isRunning = false;
		try {
			consumerProxyThread.join(timeoutMs / 2);
			// join() may timeout, in this case we should interrupt it and wait again
			if (consumerProxyThread.isAlive()) {
				consumerProxyThread.interrupt();
				consumerProxyThread.join(timeoutMs / 2);
			}
		} catch (InterruptedException e) {
			logger.warn("Join in RabbitMQConsumerProxy has failed", e);
			consumerProxyThread.interrupt();
		}
	}

	@Override
	public String toString() {
		return String.format("consumerProxy-%s-%s", this.systemName, this.channel.getChannelNumber());
	}

	// creates a separate thread for getting the messages.
	private Runnable createProxyThreadRunnable() {
		return () -> {
			isRunning = true;

			try {
				consumerThreadStartLatch.countDown();
				logger.info("Starting consumer thread {} for system {}", consumerProxyThread.getName(), systemName);

				fetchMessages();
			} catch (Throwable throwable) {
				logger.error(String.format("Error in RabbitMQConsumerProxy thread for system: %s.", systemName), throwable);
				
				// RabbitMQSystemConsumer uses the failureCause to propagate the throwable to the container
				failureCause = throwable;
				isRunning = false;
				rabbitMQSystemConsumer.setFailureCause(this.failureCause);
			}

			if (!isRunning) {
				logger.info("RabbitMQConsumerProxy for system {} has stopped.", systemName);
			}
		};
	}

	private void fetchMessages() throws IOException {
		// the actual consumption of the messages from rabbitmq
		
		boolean shouldAutoAck = false; // TODO: which are the implication with respect to Samza commit handling and offsets?
		String generatedConsumerTag = this.channel.basicConsume(this.queueName, shouldAutoAck, (String consumerTag, Delivery message) -> {
			long deliveryTag = message.getEnvelope().getDeliveryTag();
			
			IncomingMessageEnvelope incomingMessageEnvelope = processMessage(message);

			moveMessageToSamza(this.ssp, incomingMessageEnvelope);
			
			this.channel.basicAck(deliveryTag, false);
		}, (String consumerTag) -> {
			logger.error("The message consumer {} was cancelled...", consumerTag);

			// TODO: what to do here?
		});
		
		logger.info("Subscribed a new RabbitMQ consumer to receive messages from {} with consumerTag {}", this.queueName, generatedConsumerTag);
	}

	private IncomingMessageEnvelope processMessage(Delivery delivery) {
		if (delivery == null) {
			throw new SamzaException("Received null 'message' after polling consumer in RabbitMQConsumerProxy " + this);
		}

		// Parse the returned message and convert it into the IncomingMessageEnvelope.
		IncomingMessageEnvelope incomingMessageEnvelope = buildIncomingMessageEnvelope(delivery);

		return incomingMessageEnvelope;
	}

	private IncomingMessageEnvelope buildIncomingMessageEnvelope(Delivery delivery) {
		byte[] key = delivery.getProperties().getMessageId().getBytes(StandardCharsets.UTF_8);
		byte[] message = delivery.getBody();
		
		String offset = null; // TODO: implement offset handling
		int size = (int) getKeyPlusMessageSize(delivery);
		long eventTime = -1; // TODO: what value should be used if no event timestamp is available?
		if (delivery.getProperties().getTimestamp() != null) {
			eventTime = delivery.getProperties().getTimestamp().toInstant().toEpochMilli();
		}
		long arrivalTime = Instant.now().toEpochMilli();
		
		IncomingMessageEnvelope incomingMessageEnvelope = new IncomingMessageEnvelope(this.ssp, offset, key, message, size, eventTime, arrivalTime);
		
		return incomingMessageEnvelope;
	}

	protected long getKeyPlusMessageSize(Delivery delivery) {
		String messageId = delivery.getProperties().getMessageId();
		
		int messageIdSize = messageId != null ? messageId.getBytes(StandardCharsets.UTF_8).length : 0;
		long bodySize = delivery.getProperties().getBodySize();
		
		return messageIdSize + bodySize;
	}

	private void moveMessageToSamza(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
		sink.addMessage(ssp, envelope);
		
		logger.trace("Moved IncomingMessageEnvelope with offset:{} to ssp={}", envelope.getOffset(), ssp);
	}

	public static class BaseFactory<K, V> implements RabbitMQConsumerProxyFactory<K, V> {
		private final Channel channel;
		private final String systemName;

		public BaseFactory(Channel channel, String systemName) {
			this.channel = channel;
			this.systemName = systemName;
		}

		public RabbitMQConsumerProxy<K, V> create(RabbitMQSystemConsumer<K, V> rabbitMQSystemConsumer) {
			return new RabbitMQConsumerProxy<K, V>(
				rabbitMQSystemConsumer, 
				this.channel, 
				this.systemName,
				rabbitMQSystemConsumer.getMessageSink()
			);
		}
	}
}
