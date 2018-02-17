package tech.debs17gc.system.topology;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.system.input.rmq.MyRMQSource;
import tech.debs17gc.system.output.rmq.MySink;

public class ToyTopology {

	private static final Logger LOGGER = LoggerFactory.getLogger(ToyTopology.class);

	public static void main(String[] args) {
		topology();
	}

	public static void topology() {

		Config.load();
		MetaData.load();

		// TODO config also for RMQ connection
		final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		/*final SingleOutputStreamOperator<byte[]> inputStream = execEnv
				.addSource(new MyRMQSource(Common.getRabbitMQHost(), Common.getInputQueueName())).setParallelism(1).name("input");
		DataStreamSink<Tuple5<Integer, Integer, Integer, Integer, Double>> output = inputStream.addSink(new MySink(Common.getRabbitMQHost(), Common.getOutputQueueName()))
				.setParallelism(1).name("output");*/

		try {
			execEnv.execute("techdetector");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
