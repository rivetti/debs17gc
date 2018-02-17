package tech.debs17gc.system.input.soop;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

public final class SensorJoiner extends
		RichFlatMapFunction<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>, Tuple6<Integer, Long, Integer, Integer, Integer, Double>> {

	@Override
	public void flatMap(Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double> value,
			Collector<Tuple6<Integer, Long, Integer, Integer, Integer, Double>> out) throws Exception {
		// TODO Auto-generated method stub

	}

}