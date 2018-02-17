package tech.debs17gc.system.bl.opt.sort;

import org.apache.flink.api.java.tuple.Tuple5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;

public class SortedWindow {

	private final Logger LOGGER = LoggerFactory.getLogger(SortedWindow.class);

	public final int machine;
	public final int sensor;
	public final int k;

	public final long interArrivalDelay;
	public final int minSequenceLength;

	public final int windowSize;

	private long currentTS = -1;
	private boolean steadyState = false;
	private boolean terminated = false;
	private Tuple5<Integer, Integer, Long, Integer, Double> terminationTuple = null;
	private final PendingCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> pending;
	private final WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> window;

	public SortedWindow(int machine, int sensor) {
		super();

		Config.load();
		MetaData.load();
		this.interArrivalDelay = Config.getInterArrivalDelay();

		this.windowSize = Config.getWindowSize();

		//int sequence = (int) Math.ceil((Config.getMinSequenceLength() * Math.sqrt(Config.getParserParallelism())));
		//this.minSequenceLength = Math.max(this.windowSize, sequence);

		this.minSequenceLength = Config.getMinSequenceLength();

		
		this.machine = machine;
		this.sensor = sensor;
		this.k = MetaData.getMachine(machine).getSensor(sensor).k;
		this.currentTS = Long.MAX_VALUE;

		this.pending = new PendingCircularBuffer<>(this.minSequenceLength * 2, interArrivalDelay);
		this.window = new WindowCircularBuffer<>(this.windowSize);
	}

	public WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> next() throws TerminationException {
		if (terminated) {
			Tuple5<Integer, Integer, Long, Integer, Double> next = pending.remove(currentTS);// pending.remove(currentTS);
			if (next != null) {
				currentTS += interArrivalDelay;
				LOGGER.debug("[{},{}] [terminated] added to window {}, next TS: {}", machine, sensor, next, currentTS);
				this.window.add(next);
				return this.window;
			} else if (pending.isEmpty()) {
				throw new TerminationException();
			} else {
				LOGGER.debug("[{},{}] [terminated] not contiguous {}, next TS: {}", machine, sensor, next, currentTS);
			}
		} else if (steadyState) {

			Tuple5<Integer, Integer, Long, Integer, Double> next = pending.remove(currentTS);// pending.remove(currentTS);
			if (next != null) {
				currentTS += interArrivalDelay;
				LOGGER.debug("[{},{}] added to window {}, next TS: {}", machine, sensor, next, currentTS);
				this.window.add(next);
				return this.window;
			} else {
				LOGGER.debug("[{},{}] not contiguous {}, next TS: {}", machine, sensor, next, currentTS);
			}
		}

		return null;
	}

	public WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> update(
			Tuple5<Integer, Integer, Long, Integer, Double> value) throws TerminationException {
		if (steadyState) {
			return updateSteadyState(value);
		} else {
			steadyState = updateTransient(value);
			if (steadyState)
				return next();

			return null;
		}

	}

	public WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> updateSteadyState(
			Tuple5<Integer, Integer, Long, Integer, Double> value) throws TerminationException {
		if (value.f3 == -2) {
			LOGGER.debug("[{},{}] tuple: {} second termination {}", value.f0, value.f1, value, this);
			terminated = true;
			return next();
		} else if (value.f3 == -1) { // First Termination Message
			LOGGER.debug("[{},{}] tuple: {} first termination {}", value.f0, value.f1, value, this);
			terminationTuple = value;
			return next();
		} else if (value.f2 == this.currentTS) { // Fast Path
			LOGGER.debug("[{},{}] tuple: {} fast track to {}", value.f0, value.f1, value, this);
			this.window.add(value);
			currentTS += interArrivalDelay;
			return this.window;
		} else if (value.f2 < this.currentTS) { // Handle really late tuples
			LOGGER.warn("[{},{}] tuple: {} dropped", value.f0, value.f1, value, this);
			return null;
		} else {
			LOGGER.debug("[{},{}] tuple: {} added to pending {}", value.f0, value.f1, value, this);
			this.pending.put(value.f2, value);
			LOGGER.debug("[{},{}] {}", value.f0, value.f1, this.pending);
			return null;
		}

	}

	public boolean updateTransient(Tuple5<Integer, Integer, Long, Integer, Double> value) {
		LOGGER.debug("[{},{}] [transient] tuple: {} added to pending {}", value.f0, value.f1, value, this);

		this.pending.put(value.f2, value);
		LOGGER.debug("[{},{}] [transient] {}", value.f0, value.f1, this.pending);

		/*if (value.f2 == 1485903601000l) {
			return true;
		}*/
		// TODO what if machine stops and restarts?

		if (this.currentTS > value.f2) {
			LOGGER.debug("[{},{}] [transient] tuple: {} has a lower ts, update: {}", value.f0, value.f1, value, this);
			this.currentTS = value.f2;
		}
		if (pending.size() > minSequenceLength) {
			long current = currentTS;
			int sequenceLenght = 0;
			while (sequenceLenght <= minSequenceLength) {
				Tuple5<Integer, Integer, Long, Integer, Double> next = pending.get(current);// pending.remove(currentTS);pending.get(current);
				if (next != null) {
					sequenceLenght++;
					LOGGER.debug("[{},{}] sequenceLenght {}, current ({}) == next ({})", value.f0, value.f1, sequenceLenght, current, next);
					current += interArrivalDelay;
					if (sequenceLenght == minSequenceLength) {
						LOGGER.debug("[{},{}] first ts = {}", value.f0, value.f1, currentTS);
						return true;

					}
				} else {
					LOGGER.debug("[{},{}] sequenceLenght {}, current ({}) != next ({})", value.f0, value.f1, sequenceLenght, current, next);
					return false;
				}

			}

		}

		return false;

	}

	@Override
	public String toString() {
		return "MachineSensorData [currentTS=" + currentTS + ", steadyState=" + steadyState + ", pending=" + pending.size() + "]";
	}

	public class TerminationException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1049013852781378119L;

		public final Tuple5<Integer, Integer, Long, Integer, Double> tuple;

		public TerminationException() {
			super();
			this.tuple = SortedWindow.this.terminationTuple;
		}

	}

}