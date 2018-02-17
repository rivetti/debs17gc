package tech.debs17gc.system.input.oo;

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
public class BOOParser {

	private static final Logger LOGGER = LoggerFactory.getLogger(BOOParser.class);

	private final long endingDelay;
	private int inCount;
	private int outCount;
	private final Data[] parsedData;
	private int observationCount;

	/*
	 * PERF
	 */
	private double count = 0;
	private double step = 1000;
	private long delay = 0;
	private long totalDelay = 0;

	public BOOParser() {
		this.endingDelay = Config.getEndingDelay();
		this.observationCount = 117;
		this.parsedData = new Data[this.observationCount]; // TODO how much?
		for (int i = 0; i < this.parsedData.length; i++) {
			parsedData[i] = new Data(i);
		}

		this.inCount = 0;
		this.outCount = 0;
	}

	// MID, SID, FTS, LTS, VALUE
	public void flatMap(byte[] in, SourceContext<Tuple5<Integer, Integer, Long, Integer, Double>> out) {
		//LOGGER.warn("Input Data: {}", new String(in));
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

		LOGGER.trace("{}", in.length);
		Machine machine = null;
		int logicalTS = -1;
		long ts = -1;

		/*
		 * String str = new String(in); String lines[] = str.split("\n"); for (String string : lines) { LOGGER.trace("{}", string); }
		 */

		// Scan the whole bytearray
		for (int pos = 0; pos < in.length; pos++) {

			/*
			 * byte[] aux = new byte[1]; aux[0] = in[pos]; LOGGER.trace("pos: {}, char: {}", pos, new String(aux));
			 */
			int initialPos = pos;

			// Its an ObservationGroup_# line
			if (in[pos + OBSERVATION_GROUP_DF_POS] == OBSERVATION_GROUP_DF_CHAR) {
				// TODO for debug?
			}
			// Its a Cycle_# line
			else if (in[pos + CYCLE_DF_POS] == CYCLE_DF_CHAR) {
				// TODO for debug?
			}
			// Its a Timestamp_# line
			else if (logicalTS == -1 && in[pos + TS_DF_POS] == TS_DF_CHAR) {

				// Extract the TS number
				int logicalTSStart = pos + TS_LOGICAL_LEADING_OFFSET;
				int logicalTSEnd = getIndexOf(in, logicalTSStart, Common.GREATER_THAN);

				// Its the literal
				if (in[logicalTSEnd + 1 + TS_VALUE_DF_POS] == TS_VALUE_DF_CHAR) {

					int num = getIntegerValue(in, logicalTSStart, logicalTSEnd);

					LOGGER.trace("logical ts: {}", num);
					logicalTS = num;

					int tsStart = logicalTSEnd + 1 + TS_VALUE_LEADING_OFFSET;

					long value = getTS(in, tsStart);
					LOGGER.trace("ts: {} -> {}", value, new Date(value));
					ts = value;

				}

			}
			// Its a Observation_# line
			else if (in[pos + OBSERVATION_DF_POS] == OBSERVATION_DF_CHAR) {

				// Retrieve Observation Num position
				int numStart = pos + OBSERVATION_NUM_LEADING_OFFSET;
				int numEnd = getIndexOf(in, numStart, Common.GREATER_THAN);

				// Its an observedProperty line
				if (in[numEnd + 1 + OBSERVATION_PROPERTY_DF_POS] == OBSERVATION_PROPERTY_DF_CHAR) {

					// Extract Observation number
					int observationNum = getIntegerValue(in, numStart, numEnd);
					LOGGER.trace("observationNum: {}", observationNum);

					// If its an Observation number related to irrelevant sensor, then skip
					if (!parsedData[observationNum % this.observationCount].isBanned()) {

						// Extract Machine ID
						int machineStart = numEnd + 1 + OBSERVATION_NUM_SENSOR_LEADING_OFFSET;
						int machineEnd = getIndexOf(in, machineStart, Common.UNDERSCORE);

						int machineNum = getIntegerValue(in, machineStart, machineEnd);
						LOGGER.trace("machineNum: {}", machineNum);

						// Retrieve Machine metadata
						if (machine == null) {
							machine = MetaData.getMachine(machineNum);
						}

						if (machine == null) {
							LOGGER.error("Machine id is still null: {}", machineNum);
						}

						// Extract Sensor ID
						int sensorStart = machineEnd + 1;
						int sensorEnd = getIndexOf(in, sensorStart, Common.GREATER_THAN);
						int sensorNum = getIntegerValue(in, sensorStart, sensorEnd);
						LOGGER.trace("sensorNum: {}", sensorNum);

						// If the Sensor is relevant
						if (machine.sensorExists(sensorNum)) {
							parsedData[observationNum % this.observationCount].updateID(machineNum, sensorNum);
						} else {
							parsedData[observationNum % this.observationCount].ban();
						}
					}
				}

			}
			// Its a Output_# line
			else if (in[pos + OUTPUT_DF_POS] == OUTPUT_DF_CHAR) {
				// TODO for trace?
			}
			// Its a Value_# line
			else if (in[pos + VALUE_DF_POS] == VALUE_DF_CHAR) {

				// Retrieve Value number position
				int numStart = pos + VALUE_NUM_LEADING_OFFSET;
				int numEnd = getIndexOf(in, numStart, Common.GREATER_THAN);

				// Its an valueLiteral line
				if (in[numEnd + 1 + VALUE_LITERAL_DF_POS] == VALUE_LITERAL_DF_CHAR) {
					// Extract Value number
					int valueNum = getIntegerValue(in, numStart, numEnd);
					LOGGER.trace("valueNum: {}", valueNum);

					// If its a Value number related to irrelevant sensor, then skip
					if (!parsedData[valueNum % this.observationCount].isBanned()) {

						int decimalStart = numEnd + 1 + VALUE_VALUE_LEADING_OFFSET;
						int decimalEnd = getIndexOf(in, decimalStart, Common.QUOTE);
						byte[] byteValue = Arrays.copyOfRange(in, decimalStart, decimalEnd + 1);
						parsedData[valueNum % this.observationCount].updateValue(byteValue);
					}
				}

			}
			pos += MIN_LINE_LENGHT;
			for (pos = initialPos; pos < in.length && in[pos] != Common.NEW_LINE; pos++) {
				// Skip to newLine
				// TODO possible optmization moving it into the ifthenelse branches
			}

		}

		if (ts < 0 || logicalTS < 0 || machine == null) {
			throw new RuntimeException();
		}

		// TODO move in the loop ?
		for (Data data : parsedData) {
			if (!data.isBanned()) {
				LOGGER.debug("{}", data);
				double value = Double.parseDouble(new String(data.byteValue));
				Tuple5<Integer, Integer, Long, Integer, Double> tuple = new Tuple5<>(data.machine, data.sensor, ts, logicalTS, value);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("tuple: {}", new Tuple5<Integer, Integer, String, Integer, Double>(tuple.f0, tuple.f1,
							new Date(tuple.f2).toString(), tuple.f3, tuple.f4));
				}
				this.outCount++;
				out.collect(tuple);
			}
			data.reset();
		}
		delay += System.nanoTime() - start;
	}

	private static class Data {
		public final int observation;

		private boolean banned = false;

		private int machine = -1;
		private int sensor = -1;
		private byte[] byteValue = null;

		public Data(int observation) {
			super();
			this.observation = observation;
		}

		public void reset() {
			this.banned = false;
			this.machine = -1;
			this.sensor = -1;
			this.byteValue = null;
		}

		public void ban() {
			this.banned = true;
		}

		public boolean isBanned() {
			return this.banned;
		}

		public void updateValue(byte[] byteValue) {// TODO avoid byte array allocation
			this.byteValue = byteValue;
		}

		public void updateID(int machine, int sensor) {
			this.machine = machine;
			this.sensor = sensor;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + observation;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Data other = (Data) obj;
			if (observation != other.observation)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Data [observation=" + observation + ", banned=" + banned + ", machine=" + machine + ", sensor=" + sensor
					+ ", byteValue=" + byteValue + "]";
		}

	}

	private int getIndexOf(byte[] content, int start, byte byteChar) {
		for (int i = start; i < content.length; i++) {
			if (content[i] == byteChar)
				return i - 1;
		}
		throw new RuntimeException();
	}

	private int getIntegerValue(byte[] content, int start, int end) {
		int num = 0;
		for (int i = 0; i <= end - start; i++) {
			/*
			 * byte[] aux = new byte[1]; aux[0] = content[end - i]; LOGGER.trace("getIntegerValue pos: {}, char: {}", end - i, new
			 * String(aux));
			 */
			for (int j = 0; j < Common.DIGITS.length; j++) {
				if (content[end - i] == Common.DIGITS[j]) {
					num += (int) Math.round(j * Math.pow(10, i));
				}
			}

		}

		return num;

	}

	public long getTS(byte[] content, int begin) {

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

	/*
	 * public Date getDateTS(String line) { String ts = line.substring(line.length() - 1 - TIMESTAMP_TRAILING_OFFSET - TIMESTAMP_LENGHT,
	 * line.length() - 1 - TIMESTAMP_TRAILING_OFFSET);
	 * 
	 * try { Date date = TIMESTAMP_FORMAT.parse(ts); return date; } catch (ParseException e) { throw new RuntimeException(e); } }
	 */

	// private static final byte[] TERMINATION_MESSSAGE = Common.TERMINATION_MESSAGE.getBytes();
	public static final int TERMINATION_MESSAGE_DF_POS = 0;
	public static final byte TERMINATION_MESSAGE_DF_CHAR = "~".getBytes()[0];

	private static final int MIN_LINE_LENGHT = "<http://project-hobbit.eu/resources/debs2017#Output_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn#SensorOutput> ."
			.length();

	private static final int OBSERVATION_GROUP_DF_POS = "<http://project-hobbit.eu/resources/debs2017#ObservationG".length() - 1;
	private static final byte OBSERVATION_GROUP_DF_CHAR = "G".getBytes()[0];

	private static final int TS_DF_POS = "<http://project-hobbit.eu/resources/debs2017#T".length() - 1;
	private static final byte TS_DF_CHAR = "T".getBytes()[0];
	private static final int TS_VALUE_DF_POS = "> <http://www.agtinternational.com/ontologies/IoTCore#v".length() - 1;
	private static final byte TS_VALUE_DF_CHAR = "v".getBytes()[0];

	private static final int TS_LOGICAL_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Timestamp_".length();
	private static final int TS_VALUE_LEADING_OFFSET = "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"".length();

	// 2017-01-01T01:00:01+01:00
	private static final int YEAR_LENGHT = "2017".length();
	private static final int MONTH_LENGHT = "01".length();
	private static final int DAY_LENGHT = "01".length();
	private static final int HOUR_LENGHT = "01".length();
	private static final int MINUTE_LENGHT = "00".length();
	private static final int SECOND_LENGHT = "01".length();

	private static final int CYCLE_DF_POS = "<http://project-hobbit.eu/resources/debs2017#C".length() - 1;
	private static final byte CYCLE_DF_CHAR = "C".getBytes()[0];

	private static final int OBSERVATION_DF_POS = "<http://project-hobbit.eu/resources/debs2017#Observation_".length() - 1;
	private static final byte OBSERVATION_DF_CHAR = "_".getBytes()[0];
	private static final int OBSERVATION_PROPERTY_DF_POS = "> <http://purl.oclc.org/NET/ssnx/ssn#observedP".length() - 1;
	private static final byte OBSERVATION_PROPERTY_DF_CHAR = "P".getBytes()[0];

	private static final int OBSERVATION_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Observation_".length();
	private static final int OBSERVATION_NUM_SENSOR_LEADING_OFFSET = "> <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_"
			.length();

	private static final int OUTPUT_DF_POS = "<http://project-hobbit.eu/resources/debs2017#Ou".length() - 1;
	private static final byte OUTPUT_DF_CHAR = "u".getBytes()[0];

	private static final int VALUE_DF_POS = "<http://project-hobbit.eu/resources/debs2017#V".length() - 1;
	private static final byte VALUE_DF_CHAR = "V".getBytes()[0];
	private static final int VALUE_LITERAL_DF_POS = "> <http://www.agtinternational.com/ontologies/IoTCore#v".length() - 1;
	private static final byte VALUE_LITERAL_DF_CHAR = "v".getBytes()[0];
	private static final int VALUE_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Value_".length();
	private static final int VALUE_VALUE_LEADING_OFFSET = "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"".length();

}
