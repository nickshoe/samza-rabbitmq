package io.github.nickshoe.samza.system.rabbitmq;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * This is class is heavily inspired from Samza's official KafkaSystemConsumer class
 *
 * @param <K>
 * @param <V>
 */
public class RabbitMQSystemConsumer<K, V> extends BlockingEnvelopeMap implements SystemConsumer {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQSystemConsumer.class);

	protected final Connection connection;
	protected final Channel channel;
	protected final String systemName;
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final AtomicBoolean stopped = new AtomicBoolean(false);

	// This sink is used to transfer the messages from the proxy/consumer to the BlockingEnvelopeMap.
	final RabbitMQConsumerMessageSink messageSink;

	// This proxy contains a separate thread, which reads rabbitmq messages and populates BlockingEnvelopMap's buffers.
	private final RabbitMQConsumerProxy<K, V> proxy;

	private String queueName;

	public RabbitMQSystemConsumer(Connection connection, Channel channel, String systemName, RabbitMQConsumerProxyFactory<K, V> rabbitMQConsumerProxyFactory) {
		super();

		this.connection = connection;
		this.channel = channel;
		this.systemName = systemName;

		// create a sink for passing the messages between the proxy and the consumer
		this.messageSink = new RabbitMQConsumerMessageSink();

		// Create the proxy to do the actual message reading.
		this.proxy = rabbitMQConsumerProxyFactory.create(this);
		logger.info("{}: Created proxy {} ", this, proxy);
	}

	/**
	 * return system name for this consumer
	 * 
	 * @return system name
	 */
	public String getSystemName() {
		return systemName;
	}

	public RabbitMQConsumerMessageSink getMessageSink() {
		return this.messageSink;
	}

	@Override
	public void start() {
		if (!started.compareAndSet(false, true)) {
			logger.warn("{}: Attempting to start the consumer for the second (or more) time.", this);
			return;
		}

		if (stopped.get()) {
			logger.error("{}: Attempting to start a stopped consumer", this);
			return;
		}

		startConsumer();
		logger.info("{}: Consumer started", this);
	}

	@Override
	public void stop() {
		if (!stopped.compareAndSet(false, true)) {
			logger.warn("{}: Attempting to stop stopped consumer.", this);
			return;
		}

		logger.info("{}: Stopping Samza rabbitmqChannel ", this);

		// stop the proxy (with 1 minute timeout)
		if (proxy != null) {
			logger.info("{}: Stopping proxy {}", this, proxy);
			proxy.stop(TimeUnit.SECONDS.toMillis(60));
		}

		try {
			synchronized (channel) {
				logger.info("{}: Closing rabbitmqSystemConsumer {}", this, channel);
				channel.close();
			}
		} catch (Exception e) {
			logger.warn("{}: Failed to stop RabbitMQSystemConsumer.", this, e);
		}
	}

	@Override
	public void register(SystemStreamPartition systemStreamPartition, String offset) {
		if (started.get()) {
			String exceptionMessage = String.format(
					"RabbitMQSystemConsumer: %s had started. Registration of ssp: %s, offset: %s failed.", this,
					systemStreamPartition, offset);
			throw new SamzaException(exceptionMessage);
		}

		if (!Objects.equals(systemStreamPartition.getSystem(), systemName)) {
			logger.warn("{}: ignoring SSP {}, because this consumer's system doesn't match.", this,
					systemStreamPartition);
			return;
		}
		logger.info("{}: Registering ssp: {} with offset: {}", this, systemStreamPartition, offset);

		super.register(systemStreamPartition, offset);

		String streamId = systemStreamPartition.getStream();
		this.queueName = streamId;
	}

	@Override
	public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
		// check if the proxy is running
		if (!proxy.isRunning()) {
			logger.info("{}: RabbitMQConsumerProxy is not running. Stopping the consumer.", this);
			
			stop();
			
			String message = String.format("%s: RabbitMQConsumerProxy has stopped.", this);
			throw new SamzaException(message, proxy.getFailureCause());
		}

		return super.poll(systemStreamPartitions, timeout);
	}

	@Override
	protected void setFailureCause(Throwable throwable) {
		super.setFailureCause(throwable);
	}

	@Override
	public String toString() {
		return String.format("%s:%s", this.systemName, this.channel.getChannelNumber());
	}

	void startConsumer() {
		// add the partition to the proxy
		Partition partition = new Partition(0); // TODO: how to value this?
		SystemStreamPartition ssp = new SystemStreamPartition(systemName, this.queueName, partition);

		//long nextOffset = 0L; // TODO: how to value this?
		proxy.setSystemStreamPartition(ssp); 

		// start the proxy thread
		if (proxy != null && !proxy.isRunning()) {
			logger.info("{}: Starting consumer proxy thread {}", this, proxy);
			proxy.start();
		}
	}

	public class RabbitMQConsumerMessageSink {

		void addMessage(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
			logger.trace("{}: Incoming message ssp = {}: envelope = {}.", this, ssp, envelope);

			try {
				put(ssp, envelope);
			} catch (InterruptedException e) {
				throw new SamzaException(
					String.format("%s: Consumer was interrupted while trying to add message with offset %s for ssp %s", this, envelope.getOffset(), ssp)
				);
			}
		}

	}
}
