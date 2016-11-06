package mq.cinrad.kafka.consumer;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import com.google.common.collect.Queues;

@SpringBootApplication
@ComponentScan
public class ConsumerApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerApplication.class);

	private KafkaMessageListenerContainer<String, String> listenerContainer;

	@Autowired
	private DataSource cinradDataSource;

	public ConsumerApplication() {

	}

	public ConsumerApplication(DataSource cinradDataSource, Map<String, Object> rDecodeHint) {

		this.cinradDataSource = cinradDataSource;

	}

	public DataSource getCinradDataSource() {
		return cinradDataSource;
	}

	public void setCinradDataSource(DataSource cinradDataSource) {
		this.cinradDataSource = cinradDataSource;
	}

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		// TODO Auto-generated method stub

		if (null == arg0 || arg0.length == 0 || !arg0[0].endsWith(CinradConsumerConfig.CONFIG_ENDING)) {
			logger.error("请在第一个参数指定**cinrad.properties的完整路径");
			return;
		}

		logger.info(arg0[0]);
		Properties props = new Properties();
		FileInputStream fis = new FileInputStream(arg0[0]);
		props.load(fis);

		if (checkConfigFileValid(props)) {
			Set<String> topicSet = new HashSet<>();
			String topicArray = props.getProperty(CinradConsumerConfig.TOPIC_LIST);
			if (null != topicArray && !topicArray.trim().equals("")) {
				if (topicArray.contains(",")) {
					String[] ta = topicArray.trim().split(",");
					for (String s : ta) {
						if (null != s && !s.equals("")) {
							topicSet.add(s);
						}
					}
				} else {
					topicSet.add(topicArray.trim());
				}

			}
			if (topicSet.size() > 0) {

				BlockingQueue<ConsumerRecord<String, String>> recordQueue = Queues.newLinkedBlockingQueue();

				ContainerProperties containerProps = new ContainerProperties(
						topicSet.toArray(new String[topicSet.size()]));
				listenerContainer = createContainer(containerProps, props);

				CinradMessageListener listener = new CinradMessageListener(topicSet, recordQueue);

				listenerContainer.setupMessageListener(listener);

				ExecutorService service = Executors.newFixedThreadPool(2);
				CinradConsumer consumer1 = new CinradConsumer(recordQueue, cinradDataSource);
				CinradConsumer consumer2 = new CinradConsumer(recordQueue, cinradDataSource);
				service.submit(consumer1);
				service.submit(consumer2);

				listenerContainer.start();

				// logger.error(cinradDataSource.toString());

			}

		}

	}

	public void stop() {
		if (null != listenerContainer) {
			listenerContainer.stop();
		}

	}

	private boolean checkConfigFileValid(Properties props) {
		// logger.info(props.size()+"");
		for (String key : CinradConsumerConfig.CONSUMER_CONFIG_SET) {
			if (!props.containsKey(key)) {
				logger.error("配置文件**cinrad.properties有误,请检查:{}", key);
				return false;
			}
		}

		return true;

	}

	private KafkaMessageListenerContainer<String, String> createContainer(ContainerProperties containerProps,
			Properties p) {
		Map<String, Object> props = consumerProps(p);
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<String, String>(props);
		KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		return container;
	}

	private Map<String, Object> consumerProps(Properties p) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, p.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		// props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return props;
	}

}
