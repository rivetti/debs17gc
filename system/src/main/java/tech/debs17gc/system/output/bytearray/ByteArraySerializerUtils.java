package tech.debs17gc.system.output.bytearray;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.common.metadata.MetaData.Machine;
import tech.debs17gc.common.parsing.SerializeInteger;

public class ByteArraySerializerUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(ByteArraySerializerUtils.class);

	public static byte[] serialize(int anomaliesCount, Tuple5<Integer, Integer, Integer, Integer, Double> value, int transitionNum) {
		// This may be overkill since we only serialize really few tuples ...
		Machine machine = MetaData.getMachine(value.f0);
		byte[] byteTS = SerializeInteger.toByteArray(value.f3 - transitionNum);
		byte[] byteMachine = machine.byteMachine;
		byte[] byteSensor = machine.getSensor(value.f1).byteSensor;
		byte[] anomalyCount = SerializeInteger.toByteArray(anomaliesCount);
		// byte[] byteProb = VALUE_FORMATTER.format(value.f4).getBytes();// TODO did not find more efficient, however its impact may be
		byte[] byteProb = String.valueOf(value.f4).getBytes(); // negligible

		int headerSize = LEADING_BYTES.length + anomalyCount.length + HEADER_TRAILING_BYTES.length;
		int machineSize = LEADING_BYTES.length + anomalyCount.length + MACHINE_MIDST_BYTES.length + byteMachine.length
				+ TRAILING_BYTES.length;
		int sensorSize = LEADING_BYTES.length + anomalyCount.length + SENSOR_MIDST_BYTES.length + byteSensor.length + TRAILING_BYTES.length;
		int tsSize = LEADING_BYTES.length + anomalyCount.length + TS_MIDST_BYTES.length + byteTS.length + TRAILING_BYTES.length;
		int probSize = LEADING_BYTES.length + anomalyCount.length + PROB_MIDST_BYTES.length + byteProb.length + PROB_TRAILING_BYTES.length;
		int size = headerSize + machineSize + sensorSize + tsSize + probSize;

		byte[] output = new byte[size];

		LOGGER.debug("Adding Header: {}", headerSize);
		// Adding Header
		int start = 0;
		start = copyLeadingAndAnomaly(output, start, anomalyCount);
		start = copy(output, start, HEADER_TRAILING_BYTES);

		LOGGER.debug("Adding Machine: {}", machineSize);
		// Adding Machine
		start = copyLeadingAndAnomaly(output, start, anomalyCount);
		start = copy(output, start, MACHINE_MIDST_BYTES);
		start = copy(output, start, byteMachine);
		start = copy(output, start, TRAILING_BYTES);

		LOGGER.debug("Adding Sensor: {}", sensorSize);
		// Adding Sensor
		start = copyLeadingAndAnomaly(output, start, anomalyCount);
		start = copy(output, start, SENSOR_MIDST_BYTES);
		start = copy(output, start, byteSensor);
		start = copy(output, start, TRAILING_BYTES);

		LOGGER.debug("Adding TS: {}", tsSize);
		// Adding TS
		start = copyLeadingAndAnomaly(output, start, anomalyCount);
		start = copy(output, start, TS_MIDST_BYTES);
		start = copy(output, start, byteTS);
		start = copy(output, start, TRAILING_BYTES);

		LOGGER.debug("Adding Value: {}", probSize);
		// Adding Value
		start = copyLeadingAndAnomaly(output, start, anomalyCount);
		start = copy(output, start, PROB_MIDST_BYTES);
		start = copy(output, start, byteProb);
		copy(output, start, PROB_TRAILING_BYTES);

		return output;
	}

	private static int copyLeadingAndAnomaly(byte[] output, int start, byte[] anomalyCount) {
		int end = copy(output, start, LEADING_BYTES);
		return copy(output, end, anomalyCount);
	}

	private static int copy(byte[] output, int start, byte[] input) {
		LOGGER.trace("start: {}, input: {}", start, Arrays.toString(input));
		int j = start;
		for (int i = 0; i < input.length; i++, j++) {
			output[j] = input[i];
		}
		return j;
	}

	private final static String LEADING = "<http://project-hobbit.eu/resources/debs2017#Anomaly_";
	private final static byte[] LEADING_BYTES = LEADING.getBytes();
	private final static String TRAILING = "> .\n";
	private final static byte[] TRAILING_BYTES = TRAILING.getBytes();

	private final static String HEADER_TRAILING = "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> .\n";
	private final static byte[] HEADER_TRAILING_BYTES = HEADER_TRAILING.getBytes();

	private final static String MACHINE_MIDST = "> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_";
	private final static byte[] MACHINE_MIDST_BYTES = MACHINE_MIDST.getBytes();

	private final static String SENSOR_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_";
	private final static byte[] SENSOR_MIDST_BYTES = SENSOR_MIDST.getBytes();

	private final static String TS_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimeStamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_";
	private final static byte[] TS_MIDST_BYTES = TS_MIDST.getBytes();

	private final static String VALUE_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> \"";
	private final static byte[] PROB_MIDST_BYTES = VALUE_MIDST.getBytes();
	private final static String VALUE_TRAILING = "\"^^<http://www.w3.org/2001/XMLSchema#double> .";
	private final static byte[] PROB_TRAILING_BYTES = VALUE_TRAILING.getBytes();

	private final static NumberFormat VALUE_FORMATTER = new DecimalFormat();

	static {
		VALUE_FORMATTER.setMinimumIntegerDigits(1);
		VALUE_FORMATTER.setMaximumIntegerDigits(1);
		VALUE_FORMATTER.setGroupingUsed(false);
		VALUE_FORMATTER.setMinimumFractionDigits(0);
		VALUE_FORMATTER.setMaximumFractionDigits(Config.getPrecision());
		VALUE_FORMATTER.setRoundingMode(RoundingMode.HALF_UP);
	}

	//@formatter:off
    //<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_59> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_59_31> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimeStamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_24> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> "0.004115226337448559"^^<http://www.w3.org/2001/XMLSchema#double> .
	 //@formatter:on

}
