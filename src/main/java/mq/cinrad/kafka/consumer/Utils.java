package mq.cinrad.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import mq.radar.cinrad.MQFilter;

public class Utils {

	private static Map<LonLat, Integer> lonLatMap;

	private static Map<Integer, Map<String, Object>> decodeHintMap;

	public static Integer getSidByLonLat(LonLat ll) {
		if (getLonLatMap().containsKey(ll)) {
			return lonLatMap.get(ll);
		} else {
			return -999;
		}
	}

	static Map<LonLat, Integer> getLonLatMap() {
		if (null == lonLatMap) {
			lonLatMap = new HashMap<>();
			lonLatMap.put(new LonLat(113.355, 23.003), 9200);
			lonLatMap.put(new LonLat(112.563, 22.928), 9758);
		}

		return lonLatMap;

	}

	public static Map<String, Object> getDecodeHint(int pcode) {

		if (null == decodeHintMap) {
			decodeHintMap = new HashMap<>();

			decodeHintMap.put(19, getRDecodeHint());
			decodeHintMap.put(20, getRDecodeHint());
			decodeHintMap.put(37, getRDecodeHint());
			decodeHintMap.put(38, getRDecodeHint());

		}
		return decodeHintMap.get(pcode);
	}

	public static Map<String, Object> getRDecodeHint() {

		Map<String, Object> decodeHint = new HashMap<>();

		MQFilter filter = new MQFilter();
		// valueIndices是多边形color的值，color值的范围为0到15，分别对应反射率因子值0到75dBZ
		// color值为1到15都显示
		int[] valueIndices = { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

		filter.setValueIndices(valueIndices);
		decodeHint.put("cinradFilter", filter);
		decodeHint.put("reducePolygons", true);

		return decodeHint;

	}

}
