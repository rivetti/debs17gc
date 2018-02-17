package tech.debs17gc.common.parsing;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.MachineTypes;

public class ParserUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(ParserUtils.class);

	public static final int MACHINE_OBS_TRAILING_OFFSET = "MachineObservationGroup> .".length();

	public static final int MACHINE_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_0> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_"
			.length();

	public static final int OBSERVATTION_NUM_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#ObservationGroup_".length();

	public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssXXX");
	public static final int TIMESTAMP_TRAILING_OFFSET = "^^<http://www.w3.org/2001/XMLSchema#dateTime> .".length();
	public static final int TIMESTAMP_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Timestamp_".length();
	public static final int TIMESTAMP_LENGHT = "2017-01-01T01:01:30+01:00".length();

	public static final int SENSOR_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Observation_0> <http://purl.oclc.org/NET/ssnx/ssn#observedProperty> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_"
			.length();
	public static final int SENSOR_TRALING_OFFSET = "> .".length();

	public static final int SENSOR_VALUE_LEADING_OFFSET = "<http://project-hobbit.eu/resources/debs2017#Value_0> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> "
			.length();

	public Date getDateTS(String line) {
		String ts = line.substring(line.length() - 1 - TIMESTAMP_TRAILING_OFFSET - TIMESTAMP_LENGHT,
				line.length() - 1 - TIMESTAMP_TRAILING_OFFSET);

		try {
			Date date = TIMESTAMP_FORMAT.parse(ts);
			return date;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	public int getIntTS(String line) {

		int start = TIMESTAMP_LEADING_OFFSET;
		int end = StringUtils.indexOf(line, '>', start + 1);
		String num = StringUtils.substring(line, start, end);

		return Integer.parseInt(num);
	}

	public MachineTypes getMachineType(String line) {
		char c = line.charAt(line.length() - 1 - MACHINE_OBS_TRAILING_OFFSET);
		LOGGER.trace(c + "");

		switch (c) {
		case 'g': {
			return MachineTypes.MOLDING;
		}
		case 'y': {
			return MachineTypes.ASSEMBLY;
		}
		default:
			throw new RuntimeException(c + "");
		}
	}

	public int getMachineNum(String line) {
		int start = StringUtils.indexOf(line, '_', MACHINE_NUM_LEADING_OFFSET - 1) + 1;
		int end = StringUtils.indexOf(line, '>', start + 1);
		String num = StringUtils.substring(line, start, end);
		return Integer.parseInt(num);

	}

	public int getObservationGroupNum(String line) {
		int start = StringUtils.indexOf(line, '_', OBSERVATTION_NUM_LEADING_OFFSET - 1) + 1;
		int end = StringUtils.indexOf(line, '>', start + 1);
		String num = StringUtils.substring(line, start, end);
		return Integer.parseInt(num);

	}

	// Slower?
	public int getObservationGroupNum2(String line) {
		StringBuilder builder = new StringBuilder();
		for (int i = OBSERVATTION_NUM_LEADING_OFFSET; i < line.length() && line.charAt(i) != '>'; i++) {
			builder.append(line.charAt(i));
		}
		return Integer.parseInt(builder.toString());
	}

	public int getSensorNum(String line) {
		int start = StringUtils.indexOf(line, '_', SENSOR_LEADING_OFFSET - 1);
		start = StringUtils.indexOf(line, '_', start + 1) + 1;
		int end = StringUtils.indexOf(line, '>', start + 1);
		String num = StringUtils.substring(line, start, end);
		return Integer.parseInt(num);
	}

	// Slower?
	public int getSensorNum2(String line) {
		StringBuilder builder = new StringBuilder();
		for (int i = line.length() - 1 - SENSOR_TRALING_OFFSET; i >= 0 && line.charAt(i) != '_'; i--) {
			builder.append(line.charAt(i));
		}
		return Integer.parseInt(builder.reverse().toString());
	}

	public double getSensorValue(String line) {
		int start = StringUtils.indexOf(line, '"', SENSOR_VALUE_LEADING_OFFSET - 1) + 1;
		int end = StringUtils.indexOf(line, '"', start + 1);
		String num = StringUtils.substring(line, start, end);
		try {
			return Double.parseDouble(num);
		} catch (NumberFormatException e) {
			return Double.NaN;
		}
	}
}
