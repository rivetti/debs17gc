package tech.debs17gc.common.parsing;

import java.util.Date;
import java.util.List;

import tech.debs17gc.common.MachineTypes;

public class IOParserUtils extends ParserUtils {

	public int getObservationGroupNum(List<String> observationGroupData) {
		return getObservationGroupNum(observationGroupData.get(0));
	}

	public int getObservationGroupNum(String[] observationGroupData) {
		return getObservationGroupNum(observationGroupData[0]);
	}

	public MachineTypes getMachineType(List<String> observationGroupData) {
		return getMachineType(observationGroupData.get(0));
	}

	public MachineTypes getMachineType(String[] observationGroupData) {
		return getMachineType(observationGroupData[0]);
	}

	public int getMachineNum(List<String> observationGroupData) {
		return getMachineNum(observationGroupData.get(2));
	}

	public int getMachineNum(String[] observationGroupData) {
		return getMachineNum(observationGroupData[2]);
	}

	public int getIntTS(List<String> observationGroupData) {
		return getIntTS(observationGroupData.get(7));
	}

	public int getIntTS(String[] observationGroupData) {
		return getIntTS(observationGroupData[7]);
	}

	public Date getDateTS(List<String> observationGroupData) {
		return getDateTS(observationGroupData.get(7));
	}

	public Date getDateTS(String[] observationGroupData) {
		return getDateTS(observationGroupData[7]);
	}

	public int getSensorReadingStart() {
		return 8;
	}

	public int getSensorReadingLenght() {
		return 8;
	}

	public int getSensorNum(List<String> observationGroupData, int i) {
		return getSensorNum(observationGroupData.get(i + 3));
	}

	public int getSensorNum(String[] observationGroupData, int i) {
		return getSensorNum(observationGroupData[i + 3]);
	}

	public double getSensorValue(List<String> observationGroupData, int i) {
		return getSensorValue(observationGroupData.get(i + 7));
	}

	public double getSensorValue(String[] observationGroupData, int i) {
		return getSensorValue(observationGroupData[i + 7]);
	}

}
