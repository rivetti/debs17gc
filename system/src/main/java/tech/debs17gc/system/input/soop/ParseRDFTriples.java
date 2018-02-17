package tech.debs17gc.system.input.soop;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.parsing.ParserUtils;

public class ParseRDFTriples
		extends RichFlatMapFunction<Tuple3<Integer, Long, String>, Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3031236381701334277L;

	public static final ParserUtils PARSER_UTILS = new ParserUtils();

	public static final String VALUE_DF = "le> .";
	public static final String TS_DF = "me> .";
	public static final String SENSOR_DF = ".*#_(\\d+)_(\\d+)> \\.$";
	private static final Pattern pattern = Pattern.compile(SENSOR_DF);

	private static final int SENSOR_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Observation_".length();
	private static final int VALUE_OBSID_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Value_".length();
	private static final int TS_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Timestamp_".length();

	// VPID, VSID, TS, MID, OID, SID, VALUE
	@Override
	public void flatMap(Tuple3<Integer, Long, String> in, Collector<Tuple7<Integer, Long, Integer, Integer, Integer, Integer, Double>> out)
			throws Exception {

		String content = in.f2;

		// Its the sensor value
		if (StringUtils.endsWith(content, VALUE_DF)) {

			int start = VALUE_OBSID_LEADING_OFFSET;
			int end = StringUtils.indexOf(content, '>', start + 1);
			String num = StringUtils.substring(content, start, end);
			int observationId = Integer.parseInt(num);
			double value = PARSER_UTILS.getSensorValue(content);

			out.collect(new Tuple7<>(in.f0, in.f1, null, null, observationId, null, value));

		} else if (StringUtils.endsWith(content, TS_DF)) {

			int start = TS_LEADING_OFFSET;
			int end = StringUtils.indexOf(content, '>', start + 1);
			String num = StringUtils.substring(content, start, end);
			int ts = Integer.parseInt(num);

			out.collect(new Tuple7<>(in.f0, in.f1, ts, null, null, null, null));

		} else {
			Matcher matcher = pattern.matcher(content);
			if (matcher.matches()) {
				int machineId = Integer.parseInt(matcher.group(1));
				int sensorId = Integer.parseInt(matcher.group(2));

				int start = SENSOR_LEADING_OFFSET;
				int end = StringUtils.indexOf(content, '>', start + 1);
				String num = StringUtils.substring(content, start, end);
				int observationId = Integer.parseInt(num);
				out.collect(new Tuple7<>(in.f0, in.f1, null, machineId, observationId, sensorId, null));

			}
		}

	}

}
