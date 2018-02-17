package tech.debs17gc.system.output;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public interface IOutputTopology<T> {
	public DataStreamSink<T> topology(StreamExecutionEnvironment execEnv, RMQConnectionConfig rmbConfig,
			DataStream<Tuple5<Integer, Integer, Integer, Integer, Double>> inputStream);
}
