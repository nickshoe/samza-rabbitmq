package io.github.nickshoe.samza.system.rabbitmq.descriptors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.OutputDescriptorProvider;
import org.apache.samza.system.descriptors.SimpleInputDescriptorProvider;
import org.apache.samza.system.descriptors.SystemDescriptor;

import io.github.nickshoe.samza.system.rabbitmq.RabbitMQSystemFactory;

/**
 * A {@link RabbitMQSystemDescriptor} can be used for specifying Samza and RabbitMQ-specific properties of a RabbitMQ
 * input/output system. It can also be used for obtaining {@link RabbitMQInputDescriptor}s and
 * {@link RabbitMQOutputDescriptor}s, which can be used for specifying Samza and system-specific properties of
 * RabbitMQ input/output streams.
 * <p>
 * System properties provided in configuration override corresponding properties specified using a descriptor.
 * 
 * This is class is heavily inspired from Samza's official KafkaSystemDescriptor class
 */
public class RabbitMQSystemDescriptor extends SystemDescriptor<RabbitMQSystemDescriptor>
	implements SimpleInputDescriptorProvider, OutputDescriptorProvider {

	private static final String FACTORY_CLASS_NAME = RabbitMQSystemFactory.class.getName();
	private static final String HOST_CONFIG_KEY = "systems.%s.host";
	private static final String PORT_CONFIG_KEY = "systems.%s.port";
	private static final String USERNAME_CONFIG_KEY = "systems.%s.username";
	private static final String PASSWORD_CONFIG_KEY = "systems.%s.password";
	
	private Optional<String> hostOptional = Optional.empty();
	private Optional<Integer> portOptional = Optional.empty();
	private Optional<String> usernameOptional = Optional.empty();
	private Optional<String> passwordOptional = Optional.empty();
	
	/**
     * Constructs a {@link RabbitMQSystemDescriptor} instance with no system level serde.
     * Serdes must be provided explicitly at stream level when getting input or output descriptors.
     *
     * @param systemName name of this system
     */
	public RabbitMQSystemDescriptor(String systemName) {
		super(systemName, FACTORY_CLASS_NAME, null, null);
	}

	@Override
	public <StreamMessageType> RabbitMQInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, Serde<StreamMessageType> serde) {
		return new RabbitMQInputDescriptor<>(streamId, serde, this, null);
	}

	@Override
	public <StreamMessageType> RabbitMQOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
		return new RabbitMQOutputDescriptor<>(streamId, serde, this);
	}

	/**
	 * The hostname of the RabbitMQ node
	 * 
	 * @param host RabbitMQ hostname
	 * @return this system descriptor
	 */
	public RabbitMQSystemDescriptor withHost(String host) {
		this.hostOptional = Optional.of(host);
		return this;
	}
	
	/**
	 * The port of the RabbitMQ node
	 * 
	 * @param port RabbitMQ port
	 * @return this system descriptor
	 */
	public RabbitMQSystemDescriptor withPort(int port) {
		this.portOptional = Optional.of(port);
		return this;
	}
	
	/**
	 * The username for authenticating on the RabbitMQ node
	 * 
	 * @param username RabbitMQ username
	 * @return this system descriptor
	 */
	public RabbitMQSystemDescriptor withUsername(String username) {
		this.usernameOptional = Optional.of(username);
		return this;
	}
	
	/**
	 * The password for authenticating on the RabbitMQ node
	 * 
	 * @param password RabbitMQ password
	 * @return this system descriptor
	 */
	public RabbitMQSystemDescriptor withPassword(String password) {
		this.passwordOptional = Optional.of(password);
		return this;
	}
	
	@Override
	public Map<String, String> toConfig() {
		Map<String, String> parentConfig = super.toConfig();
		Map<String, String> config = new HashMap<>(parentConfig);
		
		hostOptional.ifPresent(host -> config.put(String.format(HOST_CONFIG_KEY, getSystemName()), host));
		portOptional.ifPresent(port -> config.put(String.format(PORT_CONFIG_KEY, getSystemName()), String.valueOf(port)));
		usernameOptional.ifPresent(username -> config.put(String.format(USERNAME_CONFIG_KEY, getSystemName()), username));
		passwordOptional.ifPresent(password -> config.put(String.format(PASSWORD_CONFIG_KEY, getSystemName()), password));
		
		return config;
	}
}
