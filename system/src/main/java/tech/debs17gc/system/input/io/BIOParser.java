package tech.debs17gc.system.input.io;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.common.metadata.MetaData.Machine;
import tech.debs17gc.common.metadata.MetaData.Sensor;

// TODO given that all machines have the same sensor id, avoid double lookup
public class BIOParser {

	private static final Logger LOGGER = LoggerFactory.getLogger(BIOParser.class);

	private final long endingDelay;
	private int inCount;
	private int outCount;

	/*
	 * PERF
	 */
	private double count = 0;
	private double step = 1000;
	private long delay = 0;
	private long totalDelay = 0;

	public BIOParser() {
		this.endingDelay = Config.getEndingDelay();

		this.inCount = 0;
		this.outCount = 0;
	}

	// MID, SID, FTS, LTS, VALUE
	public void flatMap(byte[] in, SourceContext<Tuple5<Integer, Integer, Long, Integer, Double>> out) {

		if (count == 0) {
			LOGGER.info("first tuple at {}", System.nanoTime());
		} else if (count > 0 && count % step == 0) {
			totalDelay += delay / step;
			LOGGER.info("count: {}, delay: {}, total: {}, avg: {}, totalAvg: {}", count, delay, totalDelay, delay / step,
					totalDelay * step / count);
			delay = 0;
		}
		count++;
		long start = System.nanoTime();

		if (in[TERMINATION_MESSAGE_DF_POS] == TERMINATION_MESSAGE_DF_CHAR) {
			LOGGER.info("last tuple at {}", System.nanoTime());
			LOGGER.info("inCount: {}", inCount);
			LOGGER.info("outCount: {}", outCount);
			LOGGER.debug("@@@@@@@@@@ Termination @@@@@@@@@@");
			for (Iterator<Entry<Integer, Machine>> machineIterator = MetaData.getMachineIterator(); machineIterator.hasNext();) {
				Entry<Integer, Machine> machineEntry = (Entry<Integer, Machine>) machineIterator.next();
				for (Iterator<Entry<Integer, Sensor>> sensorIterator = machineEntry.getValue().getSensorIterator(); sensorIterator
						.hasNext();) {
					Entry<Integer, Sensor> sensorEntry = (Entry<Integer, Sensor>) sensorIterator.next();
					Sensor sensor = sensorEntry.getValue();
					Tuple5<Integer, Integer, Long, Integer, Double> terminationTuple = new Tuple5<>(sensor.machine, sensor.id,
							Calendar.getInstance().getTimeInMillis(), -1, Double.NaN);
					LOGGER.debug("Sending 1st Termination tuple {}", terminationTuple);
					out.collect(terminationTuple);

				}

			}
			// TODO The second is needed to cleanup
			// TODO something better?
			try {
				Thread.sleep(endingDelay);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (Iterator<Entry<Integer, Machine>> machineIterator = MetaData.getMachineIterator(); machineIterator.hasNext();) {
				Entry<Integer, Machine> machineEntry = (Entry<Integer, Machine>) machineIterator.next();
				for (Iterator<Entry<Integer, Sensor>> sensorIterator = machineEntry.getValue().getSensorIterator(); sensorIterator
						.hasNext();) {
					Entry<Integer, Sensor> sensorEntry = (Entry<Integer, Sensor>) sensorIterator.next();
					Sensor sensor = sensorEntry.getValue();
					Tuple5<Integer, Integer, Long, Integer, Double> terminationTuple = new Tuple5<>(sensor.machine, sensor.id,
							Calendar.getInstance().getTimeInMillis(), -2, Double.NaN);
					LOGGER.debug("Sending 2nd (delayed) Termination tuple {}", terminationTuple);
					out.collect(terminationTuple);
				}

			}
			return;
		}

		inCount++;

		LOGGER.trace("length: {}", in.length);
		// TODO may completely remove search of next line and reading irrelevant sensors

		{
			int pos = 0;

			// Get Observation Group Num Lenght
			int ogNumLenght = -1;

			{
				int ogNumStart = pos + OG_NUM_LEADING_OFFSET;
				int ogNumEnd = getIndexOf(in, ogNumStart, Common.GREATER_THAN);
				//LOGGER.debug("ogNum {}", getIntegerValue(in, ogNumStart, ogNumEnd));
				ogNumLenght = ogNumEnd - ogNumStart + 1;
				pos += ogNumLenght + OG_NUM_TRAILING_OFFSET;
			}

			// Skip Leading Header;
			{

				LOGGER.trace("Skip Leading Header");
				pos += HEADER_LINE_1_LENGHT + ogNumLenght - 1;
				pos = getNextLine(pos, in);
				pos++;

			}

			// Get MachineNum

			int machineNum = -1;
			Machine machine = null;
			int machineNumLenght = -1;
			{
				LOGGER.trace("Get MachineNum");
				int machineNumStart = pos + MACHINE_NUM_LEADING_OFFSET + ogNumLenght - 1;
				int machineNumEnd = getIndexOf(in, machineNumStart, Common.GREATER_THAN);

				machineNum = getIntegerValue(in, machineNumStart, machineNumEnd);
				LOGGER.debug("{}", machineNum);
				machine = MetaData.getMachine(machineNum);
				LOGGER.debug("{}", machine);
				machineNumLenght = machineNumEnd - machineNumStart + 1;
				LOGGER.trace("machineNumLenght: {}", machineNumLenght);
				pos = machineNumEnd + MACHINE_NUM_TRAILING_OFFSET;
				pos++;
				pos = getNextLine(pos, in);
				pos++;
			}

			// Skip Trailing Header;
			{
				LOGGER.trace("Skip Trailing Header");
				pos += HEADER_LINE_3_LENGHT + ogNumLenght - 1;
				pos = getNextLine(pos, in);
				pos++;
				pos += HEADER_LINE_4_LENGHT;
				pos = getNextLine(pos, in);
				pos++;
				pos += HEADER_LINE_5_LENGHT;
				pos = getNextLine(pos, in);
				pos++;
				pos += HEADER_LINE_6_LENGHT;
				pos = getNextLine(pos, in);
				pos++;
			}

			// Get TS
			int logicalTS = -1;
			long ts = -1;
			{
				// Extract the TS number
				LOGGER.trace("Get TS");
				int logicalTSStart = pos + TS_LOGICAL_LEADING_OFFSET;
				int logicalTSEnd = getIndexOf(in, logicalTSStart, Common.GREATER_THAN);
				int num = getIntegerValue(in, logicalTSStart, logicalTSEnd);

				LOGGER.trace("logical ts: {}", num);
				logicalTS = num;

				int tsStart = logicalTSEnd + 1 + TS_VALUE_LEADING_OFFSET;

				long value = getTS(in, tsStart);
				LOGGER.trace("ts: {} -> {}", value, new Date(value));
				ts = value;

				pos = tsStart + TS_LENGHT;
				pos = getNextLine(pos, in);
				pos++;

			}

			for (; pos < in.length; pos++) {

				//TODO opt
				// Skip Sensor Num Header
				{
					LOGGER.trace("Skip Sensor Num Header");
					pos += OBSERVATION_LINE_0_LENGHT;
					pos = getNextLine(pos, in);
					pos++;
				}

				// Get Observation Num Lenght
				int observationNumLenght = -1;

				{
					int observatioNumStart = pos + OBSERVATION_NUM_LEADING_OFFSET;
					int obervationNumEnd = getIndexOf(in, observatioNumStart, Common.GREATER_THAN);
					//LOGGER.debug("observationNum {}", getIntegerValue(in, observatioNumStart, obervationNumEnd));
					observationNumLenght = obervationNumEnd - observatioNumStart + 1;
				}

				LOGGER.trace("observationNumLenght: {}", observationNumLenght);

				{
					LOGGER.trace("Skip Sensor Num Header2");
					pos += OBSERVATION_LINE_1_LENGHT + observationNumLenght - 1;
					;
					pos = getNextLine(pos, in);
					pos++;
					LOGGER.trace("Skip Sensor Num Header2");
					pos += OBSERVATION_LINE_2_LENGHT + 2 * (observationNumLenght - 1);
					;
					pos = getNextLine(pos, in);
					pos++;
				}

				// Extract Sensor ID
				LOGGER.debug("observationNumLenght: {} {}", observationNumLenght, machineNumLenght);
				int sensorStart = pos + OBSERVATION_NUM_LEADING_OFFSET + observationNumLenght + OBSERVATION_NUM_SENSOR_LEADING_OFFSET
						+ machineNumLenght + 1;
				int sensorEnd = getIndexOf(in, sensorStart, Common.GREATER_THAN);
				int sensorNum = getIntegerValue(in, sensorStart, sensorEnd);
				LOGGER.debug("sensorNum: {}", sensorNum);

				pos = sensorEnd + 1 + OBSERVATION_NUM_TRAILING_OFFSET;
				pos = getNextLine(pos, in);
				pos++;

				// Skip Sensor Value Header
				{
					LOGGER.trace("Skip Sensor Value Header");
					pos += OBSERVATION_LINE_4_LENGHT + observationNumLenght - 1;
					pos = getNextLine(pos, in);
					pos++;
					LOGGER.trace("Skip Sensor Value Header");
					pos += OBSERVATION_LINE_5_LENGHT + 2 * (observationNumLenght - 1);
					pos = getNextLine(pos, in);
					pos++;
					LOGGER.trace("Skip Sensor Value Header");
					pos += OBSERVATION_LINE_6_LENGHT + observationNumLenght - 1;
					pos = getNextLine(pos, in);
					pos++;
				}

				// If the Sensor is relevant
				if (machine.sensorExists(sensorNum)) {

					// Extract Sensor Value

					int decimalStart = pos + VALUE_NUM_LEADING_OFFSET + observationNumLenght + VALUE_VALUE_LEADING_OFFSET;
					int decimalEnd = getIndexOf(in, decimalStart, Common.QUOTE);
					byte[] byteValue = Arrays.copyOfRange(in, decimalStart, decimalEnd + 1);

					double value = Double.parseDouble(new String(byteValue));
					Tuple5<Integer, Integer, Long, Integer, Double> tuple = new Tuple5<>(machineNum, sensorNum, ts, logicalTS, value);
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("tuple: {}", new Tuple5<Integer, Integer, String, Integer, Double>(tuple.f0, tuple.f1,
								new Date(tuple.f2).toString(), tuple.f3, tuple.f4));
					}
					this.outCount++;
					out.collect(tuple);

					pos = decimalEnd + VALUE_VALUE_TRAILING_OFFSET;

				} else {
					// Skip Sensor Obervation
					{
						LOGGER.trace("Skip Sensor Observation");
						pos += OBSERVATION_LINE_7_LENGHT + observationNumLenght - 1;
					}

				}

				pos = getNextLine(pos, in);
			}

		}

		delay += System.nanoTime() - start;
	}

	private static int getNextLine(int initialPos, byte[] in) {

		int pos = initialPos;
		for (; pos < in.length && in[pos] != Common.NEW_LINE; pos++) {
			if (LOGGER.isTraceEnabled()) {

				byte[] aux = new byte[1];
				aux[0] = in[pos];
				LOGGER.trace("getNextLine pos: {}, char: {}", pos, new String(aux));
			}
		}
		;
		return pos;
	}

	private static int getIndexOf(byte[] content, int start, byte byteChar) {
		for (int i = start; i < content.length; i++) {
			if (content[i] == byteChar)
				return i - 1;
		}
		throw new RuntimeException();
	}

	private static int getIntegerValue(byte[] content, int start, int end) {
		int num = 0;
		for (int i = 0; i <= end - start; i++) {
			if (LOGGER.isTraceEnabled()) {

				byte[] aux = new byte[1];
				aux[0] = content[end - i];
				LOGGER.trace("getIntegerValue pos: {}, char: {}", end - i, new String(aux));
			}
			for (int j = 0; j < Common.DIGITS.length; j++) {
				if (content[end - i] == Common.DIGITS[j]) {
					num += (int) Math.round(j * Math.pow(10, i));
				}
			}

		}

		return num;

	}

	public static long getTS(byte[] content, int begin) {

		int start = begin;
		int end = begin + YEAR_LENGHT;
		int year = getIntegerValue(content, start, end - 1);
		start = end + 1;
		end = start + MONTH_LENGHT;
		int month = getIntegerValue(content, start, end - 1);
		start = end + 1;
		end = start + DAY_LENGHT;
		int day = getIntegerValue(content, start, end - 1);
		start = end + 1;
		end = start + HOUR_LENGHT;
		int hour = getIntegerValue(content, start, end - 1);
		start = end + 1;
		end = start + MINUTE_LENGHT;
		int minute = getIntegerValue(content, start, end - 1);
		start = end + 1;
		end = start + SECOND_LENGHT;
		int second = getIntegerValue(content, start, end - 1);

		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month, day, hour, minute, second);
		calendar.set(Calendar.MILLISECOND, 0);

		return calendar.getTimeInMillis();
	}

	public static final int TERMINATION_MESSAGE_DF_POS = 0;
	public static final byte TERMINATION_MESSAGE_DF_CHAR = "~".getBytes()[0];

	private static final int OG_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_".length();
	private static final int OG_NUM_TRAILING_OFFSET = "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup> ."
			.length();

	private static final int HEADER_LINE_0_LENGHT = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservationGroup> ."
			.length();
	private static final int HEADER_LINE_1_LENGHT = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://purl.oclc.org/NET/ssnx/ssn#observationResultTime> <http://project-hobbit.eu/resources/debs2017#Timestamp_0> ."
			.length();

	private static final int MACHINE_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_"
			.length();
	private static final int MACHINE_NUM_TRAILING_OFFSET = "> .".length();

	private static final int HEADER_LINE_3_LENGHT = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://www.agtinternational.com/ontologies/I4.0#observedCycle> <http://project-hobbit.eu/resources/debs2017#Cycle_0> ."
			.length();
	private static final int HEADER_LINE_4_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Cycle_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#Cycle> ."
			.length();
	private static final int HEADER_LINE_5_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Cycle_0> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> $$^^<http://www.w3.org/2001/XMLSchema#int> ."
			.length();
	private static final int HEADER_LINE_6_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Timestamp_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/IoTCore#Timestamp> ."
			.length();

	private static final int TS_LOGICAL_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Timestamp_".length();
	private static final int TS_VALUE_LEADING_OFFSET = "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"".length();

	// 2017-01-01T01:00:01+01:00
	private static final int TS_LENGHT = "2017-01-01T01:00:01+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .".length();
	private static final int YEAR_LENGHT = "2017".length();
	private static final int MONTH_LENGHT = "01".length();
	private static final int DAY_LENGHT = "01".length();
	private static final int HOUR_LENGHT = "01".length();
	private static final int MINUTE_LENGHT = "00".length();
	private static final int SECOND_LENGHT = "01".length();

	private static final int OBSERVATION_LINE_0_LENGHT = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://www.agtinternational.com/ontologies/I4.0#contains> <http://project-hobbit.eu/resources/debs2017#Observation_0> ."
			.length();
	private static final int OBSERVATION_LINE_1_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Observation_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#MoldingMachineObservation> ."
			.length();
	private static final int OBSERVATION_LINE_2_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Observation_0> <http://purl.oclc.org/NET/ssnx/ssn#observationResult> <http://project-hobbit.eu/resources/debs2017#Output_0> ."
			.length();

	private static final int OBSERVATION_LINE_4_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Output_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn#SensorOutput> ."
			.length();
	private static final int OBSERVATION_LINE_5_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Output_0> <http://purl.oclc.org/NET/ssnx/ssn#hasValue> <http://project-hobbit.eu/resources/debs2017#Value_0> ."
			.length();
	private static final int OBSERVATION_LINE_6_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Value_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/I4.0#NumberValue> ."
			.length();
	private static final int OBSERVATION_LINE_7_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Value_0> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> $$^^<http://www.w3.org/2001/XMLSchema#double> ."
			.length();

	private static final int OBSERVATION_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Observation_".length();
	private static final int OBSERVATION_NUM_SENSOR_LEADING_OFFSET = "> <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_"
			.length();
	private static final int OBSERVATION_NUM_TRAILING_OFFSET = "> .".length();

	private static final int VALUE_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Value_".length();
	private static final int VALUE_VALUE_LEADING_OFFSET = "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"".length();
	private static final int VALUE_VALUE_TRAILING_OFFSET = "^^<http://www.w3.org/2001/XMLSchema#double> .".length();

}
