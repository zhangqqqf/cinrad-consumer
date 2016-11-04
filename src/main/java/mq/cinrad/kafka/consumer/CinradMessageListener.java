package mq.cinrad.kafka.consumer;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

public class CinradMessageListener implements MessageListener<String, String> {

	private static final Logger logger = LoggerFactory.getLogger(CinradMessageListener.class);

	private BlockingQueue<ConsumerRecord<String, String>> recordQueue;

	private Set<String> topicSet;

	public CinradMessageListener(Set<String> topicSet, BlockingQueue<ConsumerRecord<String, String>> recordQueue) {
		this.recordQueue = recordQueue;
		this.topicSet = topicSet;
	}

	@Override
	public void onMessage(ConsumerRecord<String, String> data) {
		// TODO Auto-generated method stub

		if (null != data) {
			if (null != data.topic() && topicSet.contains(data.topic())) {
				try {
					recordQueue.put(data);
					logger.info("{}--Put Message into BlockingQueue,Topic:{} ,Key:{} ,Value:{}", data.offset(),
							data.topic(), data.key(), data.value());

				} catch (InterruptedException e) {

					logger.error(e.getMessage());

				}

			} else {
				logger.warn("Message Topic Not Support,Topic:{} ,Key:{} ,Value:{}", data.topic(), data.key(),
						data.value());
			}

		} else {
			logger.warn("Message Is Empty!");
		}

	}

}
