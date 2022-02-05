package io.github.nickshoe.samza.system.rabbitmq;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import io.github.nickshoe.samza.system.rabbitmq.config.RabbitMQConsumerConfig;
import io.github.nickshoe.samza.system.rabbitmq.config.RabbitMQProducerConfig;
import io.github.nickshoe.samza.system.rabbitmq.util.RabbitMQUtil;

/**
 * This is class is heavily inspired from Samza's official RabbitMQSystemFactory class
 */
public class RabbitMQSystemFactory implements SystemFactory {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQSystemFactory.class); 
	
	@Override
	public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
		// TODO: implement consumer metrics

		RabbitMQConsumerConfig rabbitMQConsumerConfig = RabbitMQConsumerConfig.getRabbitMQSystemConsumerConfig(config, systemName);
		
		Connection connection = RabbitMQUtil.createConnection(systemName, rabbitMQConsumerConfig);
		Channel channel = RabbitMQUtil.createChannel(systemName, connection);
		
		RabbitMQConsumerProxyFactory<Object, Object> rabbitMQConsumerProxyFactory = new RabbitMQConsumerProxy.BaseFactory<>(channel, systemName);
		
		RabbitMQSystemConsumer<Object, Object> rabbitMQSystemConsumer = new RabbitMQSystemConsumer<>(connection, channel, systemName, rabbitMQConsumerProxyFactory);
		
		logger.info("Created samza system consumer for system {}, config {}: {}", systemName, config, rabbitMQSystemConsumer);
		
		return rabbitMQSystemConsumer;
	}

	@Override
	public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
		// TODO: implement producer metrics
		
	    RabbitMQProducerConfig producerConfig = RabbitMQProducerConfig.getRabbitMQSystemProducerConfig(config, systemName);

	    Connection connection = RabbitMQUtil.createConnection(systemName, producerConfig.getProducerProperties());
		Channel channel = RabbitMQUtil.createChannel(systemName, connection);

	    logger.info("Creating rabbitmq producer for system {}", systemName);
	    
	    // TODO: handle the following config
	    // TaskConfig taskConfig = new TaskConfig(config);
	    // boolean dropProducerExceptions = taskConfig.getDropProducerErrors();
	    
	    RabbitMQSystemProducer rabbitMQSystemProducer = new RabbitMQSystemProducer(systemName, connection, channel);
	    
	    return rabbitMQSystemProducer;
	}

	@Override
	public SystemAdmin getAdmin(String systemName, Config config) {
		// Preso da codice di Mila Berni
		/**
		 * RabbitMQ, even if it doesn't support the concept of "partitions" natively, actually knows the concept of "offsets"; 
		 * an ad-hoc implementation is needed (see org.apache.samza.system.kafka.KafkaSystemAdmin) 
		 */
		return new SinglePartitionWithoutOffsetsSystemAdmin();
	}

}
