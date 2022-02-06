package io.github.nickshoe.samza.system.rabbitmq.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.config.Config;

import com.rabbitmq.client.ConnectionFactoryConfigurator;

public class RabbitMQProducerConfig {

	@SuppressWarnings("unused")
	private final String systemName;
	private final Map<String, String> config;

	private RabbitMQProducerConfig(String systemName, Map<String, String> config) {
		this.systemName = systemName;
		this.config = config;
	}

	public static RabbitMQProducerConfig getRabbitMQSystemProducerConfig(Config config, String systemName) {
		Config subConfig = config.subset(String.format("systems.%s.", systemName), true);
		
		Map<String, String> producerProps = new HashMap<>(subConfig);

		return new RabbitMQProducerConfig(systemName, producerProps);
	}

	public Map<String, Object> getProducerProperties() {
		Map<String, Object> producerProperties = new HashMap<String, Object>();
		
		producerProperties.put(ConnectionFactoryConfigurator.HOST, config.get("host"));
		producerProperties.put(ConnectionFactoryConfigurator.PORT, Integer.parseInt(config.get("port")));
		producerProperties.put(ConnectionFactoryConfigurator.USERNAME, config.get("username"));
		producerProperties.put(ConnectionFactoryConfigurator.PASSWORD, config.get("password"));
		
		return producerProperties;
	}

}
