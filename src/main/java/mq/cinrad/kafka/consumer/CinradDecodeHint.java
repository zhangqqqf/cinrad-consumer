package mq.cinrad.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import mq.radar.cinrad.MQFilter;

@Configuration
@ConfigurationProperties(prefix = "cinrad.decodeHint")
public class CinradDecodeHint {

	private static int[] rValueIndices;
	private static Boolean rReducePolygons;

	public int[] getrValueIndices() {
		return rValueIndices;
	}

	@SuppressWarnings("static-access")
	public void setrValueIndices(int[] rValueIndices) {
		this.rValueIndices = rValueIndices;
	}

	public Boolean getrReducePolygons() {
		return rReducePolygons;
	}

	@SuppressWarnings("static-access")
	public void setrReducePolygons(Boolean rReducePolygons) {
		this.rReducePolygons = rReducePolygons;
	}

	public static Map<String, Object> rDecodeHint() {

		Map<String, Object> decodeHint = new HashMap<>();

		if (null != rValueIndices) {
			MQFilter filter = new MQFilter();
			filter.setValueIndices(rValueIndices);
			decodeHint.put("cinradFilter", filter);
		}

		if (null != rReducePolygons) {
			decodeHint.put("reducePolygons", rReducePolygons);
		}

		return decodeHint;

	}

}
