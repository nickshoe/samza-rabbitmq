package io.github.nickshoe.samza.system.rabbitmq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;

/**
 * This class is heavily inspired to Samza official KafkaConsumerConfig class
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
	 * @param config application config
	 * @param systemName system name
	 * @return RabbitMQConsumerConfig the consumer config
	 */
	public static RabbitMQConsumerConfig getRabbitMQSystemConsumerConfig(Config config, String systemName) {

		Config subConf = config.subset(String.format("systems.%s.consumer.", systemName), true);

		Map<String, Object> consumerProps = new HashMap<>(subConf);

		// TODO: Disable consumer auto-commit because Samza controls commits

		// TODO: Implement offset handling  

		// Translate samza config value to rabbitmq config value
		String host = config.get(String.format("systems.%s.%s", systemName, "host"));
		consumerProps.put(ConnectionFactoryConfigurator.HOST, host);
		
		Integer port = Integer.parseInt(config.get(String.format("systems.%s.%s", systemName, "port")));
		consumerProps.put(ConnectionFactoryConfigurator.PORT, port);

		String username = config.get(String.format("systems.%s.%s", systemName, "username"));
		consumerProps.put(ConnectionFactoryConfigurator.USERNAME, username);
		
		String password = config.get(String.format("systems.%s.%s", systemName, "password"));
		consumerProps.put(ConnectionFactoryConfigurator.PASSWORD, password);

		consumerProps.put(ConnectionFactoryConfigurator.VIRTUAL_HOST, ConnectionFactory.DEFAULT_VHOST);
		
		return new RabbitMQConsumerConfig(consumerProps);
	}

}
