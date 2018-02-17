package tech.debs17gc.system.input.sio;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.parsing.ParserUtils;

public class SIOParser extends RichFlatMapFunction<String, Tuple4<Integer, Integer, Integer, Double>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9034787702826433639L;

	private static final Logger LOGGER = LoggerFactory.getLogger(ParserUtils.class);

	private final ParserUtils parserUtils = new ParserUtils();

	private int getMachineNum(String[] observationGroupData) {
		String line = observationGroupData[2];
		LOGGER.trace(line);

		int machineNum = parserUtils.getMachineNum(line);
		LOGGER.debug("Machine Num: {}", machineNum);
		return machineNum;
	}

	private int getTS(String[] observationGroupData) {
		String line = observationGroupData[7];
		int ts = parserUtils.getIntTS(line);
		LOGGER.debug("TS: {}", ts);
		return ts;

	}

	private int getSensorNum(String[] observationGroupData, int i) {
		String line = observationGroupData[i + 3];

		int sensorNum = parserUtils.getSensorNum(line);
		LOGGER.debug("Sensor Num: {}", sensorNum);
		return sensorNum;
	}

	private double getSensorValue(String[] observationGroupData, int i) {
		String line = observationGroupData[i + 7];

		double sensorValue = parserUtils.getSensorValue(line);
		LOGGER.debug("Sensor Value: {}", sensorValue);
		return sensorValue;
	}

	@Override
	public void flatMap(String value, Collector<Tuple4<Integer, Integer, Integer, Double>> out) throws Exception {

		String[] observationGroupData = StringUtils.split("\n");

		// Machine num
		int machineNum = getMachineNum(observationGroupData);

		int ts = getTS(observationGroupData);

		for (int i = 8; i < observationGroupData.length; i += 8) {
			LOGGER.trace(observationGroupData[i]);

			// Get Sensor num
			int sensorNum = getSensorNum(observationGroupData, i);

			// Get Sensor value
			double sensorValue = getSensorValue(observationGroupData, i);

			out.collect(new Tuple4<>(machineNum, ts, sensorNum, sensorValue));
		}

	}

}
