package mq.cinrad.kafka.consumer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class CinradConsumerConfig {

	public static final String CONFIG_ENDING = "cinrad.properties";

	public static final String BOOTSTRAP_SERVERS_CONFIG = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

	public static final String TOPIC_LIST = "topic.list";
	
	public static final String GROUP_ID_CONFIG=ConsumerConfig.GROUP_ID_CONFIG;

	public static final String[] CONSUMER_CONFIG_ARRAY = { TOPIC_LIST, BOOTSTRAP_SERVERS_CONFIG,GROUP_ID_CONFIG };

	public static final Set<String> CONSUMER_CONFIG_SET = new HashSet<>(Arrays.asList(CONSUMER_CONFIG_ARRAY));

}
