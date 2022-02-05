package io.github.nickshoe.samza.system.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * This is class is heavily inspired from Samza's official KafkaConsumerProxy class
 *
 * @param <K>
 * @param <V>
 */
public class RabbitMQConsumerProxy<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQSystemConsumer.class);

	private final Thread consumerPollThread;
	private final Channel channel;
	private final RabbitMQSystemConsumer<K, V> rabbitMQSystemConsumer;
	private final RabbitMQSystemConsumer<K, V>.RabbitMQConsumerMessageSink sink;
	private final String systemName;

	private String queue;
	private SystemStreamPartition ssp;

	private volatile boolean isRunning = false;
	private volatile Throwable failureCause = null;
	private final CountDownLatch consumerPollThreadStartLatch = new CountDownLatch(1);

	public RabbitMQConsumerProxy(RabbitMQSystemConsumer<K, V> rabbitMQSystemConsumer, Channel channel,
			String systemName, RabbitMQSystemConsumer<K, V>.RabbitMQConsumerMessageSink messageSink) {

		this.rabbitMQSystemConsumer = rabbitMQSystemConsumer;
		this.sink = messageSink;

		this.channel = channel;
		this.systemName = systemName;

		this.consumerPollThread = new Thread(createProxyThreadRunnable());
		this.consumerPollThread.setDaemon(true);
		this.consumerPollThread
				.setName("Samza RabbitMQConsumerProxy Poll " + consumerPollThread.getName() + " - " + systemName);

		logger.info("Creating RabbitMQConsumerProxy with systeName={}", systemName);
	}

	public boolean isRunning() {
		return isRunning;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	/**
	 * Add new partition to the list of polled partitions. 
	 * Must be called before {@link RabbitMQConsumerProxy#start} is called.
	 * 
	 * @param ssp        - SystemStreamPartition to add
	 */
	public void addQueue(SystemStreamPartition ssp) {
		logger.info(String.format("Adding new stream partition %s to queue for consumer %s", ssp, this));

		String queue = ssp.getStream();

		this.queue = queue;
		this.ssp = ssp;
	}

	public void start() {
		if (!consumerPollThread.isAlive()) {
			logger.info("Starting RabbitMQConsumerProxy polling thread for " + this.toString());

			consumerPollThread.start();

			// we need to wait until the thread starts
			while (!isRunning && failureCause == null) {
				try {
					consumerPollThreadStartLatch.await(3000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					logger.info("Ignoring InterruptedException while waiting for consumer poll thread to start.", e);
				}
			}
		} else {
			logger.warn("Tried to start an already started KafkaConsumerProxy (%s). Ignoring.", this.toString());
		}
	}

	/**
	 * Stop this RabbitMQConsumerProxy and wait for at most {@code timeoutMs}.
	 * 
	 * @param timeoutMs maximum time to wait to stop this RabbitMQConsumerProxy
	 */
	public void stop(long timeoutMs) {
		logger.info("Shutting down RabbitMQConsumerProxy poll thread {} for {}", consumerPollThread.getName(), this);

		isRunning = false;
		try {
			consumerPollThread.join(timeoutMs / 2);
			// join() may timeout
			// in this case we should interrupt it and wait again
			if (consumerPollThread.isAlive()) {
				consumerPollThread.interrupt();
				consumerPollThread.join(timeoutMs / 2);
			}
		} catch (InterruptedException e) {
			logger.warn("Join in RabbitMQConsumerProxy has failed", e);
			consumerPollThread.interrupt();
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
				consumerPollThreadStartLatch.countDown();
				logger.info("Starting consumer poll thread {} for system {}", consumerPollThread.getName(), systemName);

				//while (isRunning) {
					fetchMessages();
				//}
			} catch (Throwable throwable) {
				logger.error(String.format("Error in RabbitMQConsumerProxy poll thread for system: %s.", systemName),
						throwable);
				// RabbitMQSystemConsumer uses the failureCause to propagate the throwable to
				// the container
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
		boolean shouldAutoAck = true; // TODO: from config
		this.channel.basicConsume(this.queue, shouldAutoAck, (String consumerTag, Delivery message) -> {
			Map<SystemStreamPartition, List<IncomingMessageEnvelope>> response = processMessage(message);

			// move the responses into the queue
			for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> e : response.entrySet()) {
				List<IncomingMessageEnvelope> envelopes = e.getValue();
				if (envelopes != null) {
					moveMessagesToTheirQueue(e.getKey(), envelopes);
				}
			}
		}, (String consumerTag) -> {
			logger.error("The message consumer {} was cancelled...", consumerTag);

			// TODO: how to restart in samza?
		});

	}

	private Map<SystemStreamPartition, List<IncomingMessageEnvelope>> processMessage(Delivery delivery) {
		if (delivery == null) {
			throw new SamzaException("Received null 'message' after polling consumer in RabbitMQConsumerProxy " + this);
		}

		Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = new HashMap<>(1);
		List<IncomingMessageEnvelope> incomingMessagesEnvelopes = results.computeIfAbsent(ssp, k -> new ArrayList<>());

		// Parse the returned message and convert it into the IncomingMessageEnvelope.
		IncomingMessageEnvelope incomingMessageEnvelope = buildIncomingMessageEnvelope(delivery);

		incomingMessagesEnvelopes.add(incomingMessageEnvelope);

		if (logger.isDebugEnabled()) {
			logger.debug("# records per SSP:");
			for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> e : results.entrySet()) {
				List<IncomingMessageEnvelope> list = e.getValue();
				logger.debug(e.getKey() + " = " + ((list == null) ? 0 : list.size()));
			}
		}

		return results;
	}

	private IncomingMessageEnvelope buildIncomingMessageEnvelope(Delivery delivery) {
		byte[] key = delivery.getProperties().getMessageId().getBytes(StandardCharsets.UTF_8);
		byte[] message = delivery.getBody();
		
		String offset = null; // TODO: how to value this?
		int size = getKeyPlusMessageSize(delivery); // TODO: compute this based on key and message objects
		long eventTime = -1; // TODO: ?
		if (delivery.getProperties().getTimestamp() != null) {
			eventTime = delivery.getProperties().getTimestamp().toInstant().toEpochMilli();
		}
		long arrivalTime = Instant.now().toEpochMilli();
		IncomingMessageEnvelope incomingMessageEnvelope = new IncomingMessageEnvelope(this.ssp, offset, key, message, size, eventTime, arrivalTime);
		return incomingMessageEnvelope;
	}

	/**
	 * TODO: compute this based on key and message objects
	 * 
	 * @param delivery
	 * @return
	 */
	protected int getKeyPlusMessageSize(Delivery delivery) {
		String messageId = delivery.getProperties().getMessageId();
		int messageIdSize = messageId != null ? messageId.getBytes(StandardCharsets.UTF_8).length : 0;
		int bodySize = (int) delivery.getProperties().getBodySize(); // TODO: ?
		return messageIdSize + bodySize;
	}

	private void moveMessagesToTheirQueue(SystemStreamPartition ssp, List<IncomingMessageEnvelope> envelopes) {
		for (IncomingMessageEnvelope envelope : envelopes) {
			sink.addMessage(ssp, envelope); // move message to the BlockingEnvelopeMap's queue

			logger.trace("IncomingMessageEnvelope. got envelope with offset:{} for ssp={}", envelope.getOffset(), ssp);
		}
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
