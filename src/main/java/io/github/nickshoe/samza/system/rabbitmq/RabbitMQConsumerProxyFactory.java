package io.github.nickshoe.samza.system.rabbitmq;

/**
 * This is class is heavily inspired from Samza's official KafkaConsumerProxyFactory class
 *
 * @param <K>
 * @param <V>
 */
public interface RabbitMQConsumerProxyFactory<K, V> {
	RabbitMQConsumerProxy<K, V> create(RabbitMQSystemConsumer<K, V> rabbitMQSystemConsumer);
}
