package com.mohanty.kafkaconsumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.ibm.dgwservice.modelsJobDetails.JobDetails;

@Service
public class KafkaListenerService {
	
	@Value("${kafka.bootstrapAddress}")
	private String bootstrapAddress;
	
	@Value("${message.topic.name}")
	private String topicName;
	
	@Value("${message.topic.groupid}")
	private String groupId;
	
	private static final Logger LOGGER = LoggerFactory.getLogger("KafkaListenerService");
	
	@KafkaListener(topics = "${message.topic.name}", groupId = "${message.topic.groupid}",
			containerFactory = "jobDetailsContainerFactory")
	public void consumerMessage(JobDetails jobdetails) {
		try {
			LOGGER.info("Consuming the message from kafka topic...");
			System.out.println("Message consumed: " + jobdetails.getHostTrackingNumber());
			
		} catch(Exception e) {
			LOGGER.error("Exception in consuming the message from kafka topic");
			e.printStackTrace();
		}
	}

}
