package tech.debs17gc.system.bl;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface IBLTopology {
	public DataStream<Tuple5<Integer, Integer, Integer, Integer, Double>> topology(
			DataStream<Tuple5<Integer, Integer, Long, Integer, Double>> inputStream);
}
