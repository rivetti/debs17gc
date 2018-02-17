package tech.debs17gc.system.input;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public interface IParserTopology<T> {
	public DataStream<T> topology(StreamExecutionEnvironment execEnv,
			RMQConnectionConfig rmbConfig);
}
