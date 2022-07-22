package com.mohanty.kafkaconsumer.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.ibm.dgwservice.modelsJobDetails.JobDetails;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

	@Value("${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	@Value("${message.topic.name}")
	private String topicName;

	@Value("${message.topic.groupid}")
	private String groupId;

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {

		Map<String, Object> configs = new HashMap<>();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(configs);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> containerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

	@Bean
	public ConsumerFactory<String, JobDetails> jobDetailsConsumerFactory() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(),
				new JsonDeserializer<>(JobDetails.class));
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, JobDetails> jobDetailsContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, JobDetails> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(jobDetailsConsumerFactory());
		return factory;
	}

}
