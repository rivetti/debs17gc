package tech.debs17gc.system.input.soop;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import tech.debs17gc.common.Common;
import tech.debs17gc.system.input.IParserTopology;

public class SOOPParserTopology implements IParserTopology {

	@Override
	public DataStream<Tuple4<Integer, Integer, Integer, Double>> topology(StreamExecutionEnvironment execEnv,
			RMQConnectionConfig rmbConfig) {

		// Connector from RabbitMQ
		final DataStream<String> inputStream = execEnv
				.addSource(new RMQSource<String>(rmbConfig, Common.getInputQueueName(), true, new SimpleStringSchema()));

		// Split the ObservationGroup in single RDF triplets
		DataStream<Tuple3<Integer, Long, String>> RDFTriplesStream = inputStream.flatMap(new ObservationGroup2RDFTriplets());

		final String tsStreamId = "t";
		final String sensorStreamId = "s";

		SplitStream<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>> parsedStream = RDFTriplesStream
				.flatMap(new ParseRDFTriples())
				.split(new OutputSelector<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2498061682148885606L;

					@Override
					public Iterable<String> select(Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double> in) {
						List<String> output = new ArrayList<String>();
						if (in.f2 != null) {
							output.add(tsStreamId);
						} else {
							output.add(sensorStreamId);
						}
						return null;
					}
				});

		DataStream<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>> tsStream = parsedStream.select(tsStreamId);

		DataStream<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>> sensorStream = parsedStream.select(sensorStreamId)
				.keyBy(0, 1, 4);

		return null;

	}

}
