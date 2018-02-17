package tech.debs17gc.common;

import org.apache.flink.api.java.tuple.Tuple;

public class MachineSensorKey {

	public final int machine;
	public final int sensor;

	public MachineSensorKey(int machine, int sensor) {
		super();
		this.machine = machine;
		this.sensor = sensor;
	}

	public MachineSensorKey(Tuple tuple) {
		this(tuple.getField(0), tuple.getField(1));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + machine;
		result = prime * result + sensor;
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
		MachineSensorKey other = (MachineSensorKey) obj;
		if (machine != other.machine)
			return false;
		if (sensor != other.sensor)
			return false;
		return true;
	}

}
