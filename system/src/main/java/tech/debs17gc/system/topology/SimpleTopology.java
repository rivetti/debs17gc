package tech.debs17gc.system.topology;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.system.bl.opt.BusinessLogic;
import tech.debs17gc.system.input.io.RMQSourceBIOParser;
import tech.debs17gc.system.input.oo.RMQSourceBOOParser;
import tech.debs17gc.system.output.bytearray.ByteArraySerializer;
import tech.debs17gc.system.output.rmq.MySink;

public class SimpleTopology {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTopology.class);

	public static void main(String[] args) {
		topology();
	}

	public static void topology() {
		Config.load();
		MetaData.load();

		final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * final SingleOutputStreamOperator<byte[]> inputStream = execEnv .addSource(new MyRMQSourceMultiple(Common.getRabbitMQHost(),
		 * Common.getInputQueueName()), "input", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
		 * .setParallelism(Config.getSourceParallelism()).slotSharingGroup(Config.getSourceGroup()).setBufferTimeout(Config.getSourceTimeout
		 * ());
		 */

		final SingleOutputStreamOperator<Tuple5<Integer, Integer, Long, Integer, Double>> parsedStream = execEnv
				.addSource(new RMQSourceBOOParser(Common.getRabbitMQHost(), Common.getInputQueueName()),
						TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Integer.class, Long.class, Integer.class, Double.class))
				.setParallelism(Config.getParserParallelism()).slotSharingGroup(Config.getParserGroup())
				.setBufferTimeout(Config.getParserTimeout()).name("parsed");

		Config.parserChaining(parsedStream);

		final SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Double>> anomalies = parsedStream.keyBy(0, 1)
				.flatMap(new BusinessLogic()).setParallelism(Config.getBLParallelism()).slotSharingGroup(Config.getBLGroup())
				.setBufferTimeout(Config.getBLTimeout()).name("bl");

		Config.blChaining(anomalies);

		final SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Double>> serialized = anomalies.flatMap(new ByteArraySerializer())
				.setParallelism(Config.getSerializerParallelism()).slotSharingGroup(Config.getSerializerGroup())
				.setBufferTimeout(Config.getSerializerTimeout()).name("serialized");

		Config.serializerChaining(serialized);
		DataStreamSink<Tuple5<Integer, Integer, Integer, Integer, Double>> output = serialized.addSink(new MySink(Common.getRabbitMQHost(), Common.getOutputQueueName()))
				.setParallelism(Config.getSinkParallelism()).slotSharingGroup(Config.getSinkGroup()).name("output");

		Config.sinkChaining(output);

		LOGGER.info(execEnv.getExecutionPlan());

		try {
			execEnv.execute("techdetector");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
