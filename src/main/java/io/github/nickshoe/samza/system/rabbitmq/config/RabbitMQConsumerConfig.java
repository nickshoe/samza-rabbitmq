package io.github.nickshoe.samza.system.rabbitmq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactoryConfigurator;

/**
 * This is class is heavily inspired from Samza's official KafkaConsumerConfig class
 *
 */
public class RabbitMQConsumerConfig extends HashMap<String, Object> {

	private static final long serialVersionUID = 1L;

	public static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerConfig.class);

	private RabbitMQConsumerConfig(Map<String, Object> props) {
		super(props);
	}

	/**
	 * Create rabbitmq consumer configs, based on the subset of global configs.
	 * 
	 * @param config     application config
	 * @param systemName system name
	 * @param clientId   client id provided by the caller
	 * @return RabbitMQConsumerConfig
	 */
	public static RabbitMQConsumerConfig getRabbitMQSystemConsumerConfig(Config config, String systemName) {

		Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

		Map<String, Object> consumerProps = new HashMap<>(subConf);

		// Disable consumer auto-commit because Samza controls commits
		// TODO: ? (see KafkaConsumerConfig)

		// check if samza default offset value is defined
		// TODO: ? (see KafkaConsumerConfig)
		//String systemOffsetDefault = new SystemConfig(config).getSystemOffsetDefault(systemName);

		// Translate samza config value to rabbitmq config value
		// TODO: ? (see KafkaConsumerConfig)
		//logger.info("setting auto.offset.reset for system {} to {}", systemName, autoOffsetReset);

		// TODO: allow to specify different rabbitmq connection configs for consumer and producer
		String host = config.get(String.format("systems.%s.%s", systemName, "host"));
		consumerProps.put(ConnectionFactoryConfigurator.HOST, host);
		
		Integer port = Integer.parseInt(config.get(String.format("systems.%s.%s", systemName, "port")));
		consumerProps.put(ConnectionFactoryConfigurator.PORT, port);

		String username = config.get(String.format("systems.%s.%s", systemName, "username"));
		consumerProps.put(ConnectionFactoryConfigurator.USERNAME, username);
		
		String password = config.get(String.format("systems.%s.%s", systemName, "password"));
		consumerProps.put(ConnectionFactoryConfigurator.PASSWORD, password);

		// TODO: set virtual host?
		
		String idDeserializer = config.get(String.format("systems.%s.consumer.%s", systemName, "id-deserializer"));
		consumerProps.put("id-deserializer", idDeserializer);
		
		String bodyDeserializer = config.get(String.format("systems.%s.consumer.%s", systemName, "body-deserializer"));
		consumerProps.put("body-deserializer", bodyDeserializer);
		
		return new RabbitMQConsumerConfig(consumerProps);
	}

}
