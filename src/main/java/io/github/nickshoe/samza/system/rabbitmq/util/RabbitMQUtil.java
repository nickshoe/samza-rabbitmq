package io.github.nickshoe.samza.system.rabbitmq.util;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionFactoryConfigurator;

public class RabbitMQUtil {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQUtil.class);
	
	/**
	 * Create internal RabbitMQ connection object.
	 * 
	 * @param systemName
	 * @param config
	 * @return the created connection object
	 */
	public static Connection createConnection(String systemName, Map<String, Object> config) {
		logger.info("Instantiating Connection for systemName {} with properties {}", systemName, config);

		final ConnectionFactory factory = new ConnectionFactory();

		factory.setHost((String) config.get(ConnectionFactoryConfigurator.HOST));
		factory.setPort((Integer) config.get(ConnectionFactoryConfigurator.PORT));
		factory.setUsername((String) config.get(ConnectionFactoryConfigurator.USERNAME));
		factory.setPassword((String) config.get(ConnectionFactoryConfigurator.PASSWORD));

		try {
			// the actual TCP connection to the broker
			Connection connection = factory.newConnection();
			logger.debug("Connection established with the broker: {}",
					connection.getAddress().getHostName() + ":" + connection.getPort());

			return connection;
		} catch (IOException | TimeoutException e) {
			logger.error("An error occurred while creating the rabbitmq connection: {}", e.getMessage());
			e.printStackTrace();

			throw new SamzaException(e);
		}
	}

	/**
	 * Create internal RabbitMQ channel object, which will be used in the Proxy.
	 * 
	 * @param systemName
	 * @param connection
	 * @return the created channel object
	 */
	public static Channel createChannel(String systemName, Connection connection) {
		logger.info("Instantiating Channel for systemName {}", systemName);

		try {
			// a channel can be thought of as "lightweight connections that share a single TCP connection"
			Channel channel = connection.createChannel();
			logger.debug("Channel created over the existing connection: {}", channel.getChannelNumber());

			return channel;
		} catch (IOException e) {
			logger.error("An error occurred while creating the rabbitmq channel: {}", e.getMessage());
			e.printStackTrace();

			throw new SamzaException(e);
		}
	}
	
}
