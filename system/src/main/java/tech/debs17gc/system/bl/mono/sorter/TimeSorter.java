package tech.debs17gc.system.bl.mono.sorter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;

public class TimeSorter
		extends RichFlatMapFunction<Tuple5<Integer, Integer, Long, Integer, Double>, Tuple4<Integer, Integer, Integer, Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1707778057249451058L;

	private static final Logger LOGGER = LoggerFactory.getLogger(TimeSorter.class);

	private HashMap<Integer, HashMap<Integer, MachineSensorData>> pending;
	private long initialDelay;
	private long interArrivalDelay;
	private int minSequenceLength;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		LOGGER.error("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		Config.load();
		MetaData.load();

		this.initialDelay = Config.getInitialDelay();
		this.interArrivalDelay = Config.getInterArrivalDelay();
		this.minSequenceLength = Config.getMinSequenceLength();

		this.pending = new HashMap<>();
	}

	@Override
	public void flatMap(Tuple5<Integer, Integer, Long, Integer, Double> value, Collector<Tuple4<Integer, Integer, Integer, Double>> out)
			throws Exception {
		// System.out.println(value);
		LOGGER.debug("[{},{}] thread: {} ({}),  tuple: {}", value.f0, value.f1, Thread.currentThread().getName(),
				Thread.currentThread().getId(), value);
		this.pending.putIfAbsent(value.f0, new HashMap<>());
		this.pending.get(value.f0).putIfAbsent(value.f1, new MachineSensorData(value));
		MachineSensorData data = this.pending.get(value.f0).get(value.f1);
		data.update(value);
		LOGGER.debug("[{},{}] tuple Data: {}", value.f0, value.f1, data);
		if (data.steadyState) {
			Iterator<Entry<Long, Tuple5<Integer, Integer, Long, Integer, Double>>> it = data.pending.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Long, Tuple5<Integer, Integer, Long, Integer, Double>> next = it.next();
				if (data.currentTS == next.getKey()) {
					data.currentTS += interArrivalDelay;
					Tuple4<Integer, Integer, Integer, Double> tuple = new Tuple4<>(next.getValue().f0, next.getValue().f1,
							next.getValue().f3, next.getValue().f4);
					LOGGER.debug("[{},{}] sent {}, next TS: {}", value.f0, value.f1, tuple, data.currentTS);
					it.remove();
					out.collect(tuple);
				} else {
					break;
				}
			}

		} else if (data.terminationTuple != null) {
			Iterator<Entry<Long, Tuple5<Integer, Integer, Long, Integer, Double>>> it = data.pending.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Long, Tuple5<Integer, Integer, Long, Integer, Double>> next = it.next();
				Tuple4<Integer, Integer, Integer, Double> tuple = new Tuple4<>(next.getValue().f0, next.getValue().f1, next.getValue().f3,
						next.getValue().f4);
				it.remove();
				out.collect(tuple);

			}
			Tuple4<Integer, Integer, Integer, Double> tuple = new Tuple4<>(data.terminationTuple.f0, data.terminationTuple.f1,
					data.terminationTuple.f3, data.terminationTuple.f4);
			out.collect(tuple);
		}
	}

	public class MachineSensorData {
		public long currentTS = -1;
		public long waitingTS = -1;
		public boolean steadyState = false;
		public Tuple5<Integer, Integer, Long, Integer, Double> terminationTuple = null;
		public TreeMap<Long, Tuple5<Integer, Integer, Long, Integer, Double>> pending;

		public MachineSensorData(Tuple5<Integer, Integer, Long, Integer, Double> value) {
			super();
			this.currentTS = value.f2;
			this.waitingTS = System.currentTimeMillis() + initialDelay;
			this.pending = new TreeMap<>();
			this.pending.put(value.f2, value);
		}

		public void update(Tuple5<Integer, Integer, Long, Integer, Double> value) {
			if (value.f3 == -1) {
				terminationTuple = value;
				return;
			} else if (value.f3 == -2) {
				steadyState = false;
				return;
			}

			this.pending.put(value.f2, value);
			LOGGER.debug("[{},{}] tuple: {} added to {}", value.f0, value.f1, value, this);
			if (!steadyState) {
				LOGGER.debug("[{},{}] not steady state", value.f0, value.f1);
				if (this.currentTS > value.f2) {
					this.currentTS = value.f2;
					this.waitingTS = System.currentTimeMillis() + initialDelay;
					LOGGER.debug("[{},{}] tuple: {} has a lower ts, update: {}", value.f0, value.f1, value, this);
				} else {
					LOGGER.debug("[{},{}] tuple: {} has a not lower ts", value.f0, value.f1, value);
					if (System.currentTimeMillis() >= this.waitingTS) {
						LOGGER.debug("[{},{}] time elapsed: {}  >= {}", value.f0, value.f1, System.currentTimeMillis(), this.waitingTS);
						Iterator<Long> it = pending.keySet().iterator();

						long current = currentTS;
						for (int i = 0; i < minSequenceLength && it.hasNext(); i++) {
							long next = it.next();
							if (current == next) {
								LOGGER.debug("[{},{}] iteration {}, current ({}) == next ({})", value.f0, value.f1, i, current, next);
							} else {
								LOGGER.debug("[{},{}] iteration {}, current ({}) != next ({})", value.f0, value.f1, i, current, next);
							}
							current += interArrivalDelay;
						}

						if (it.hasNext()) {
							long next = it.next();
							if (current == next) {
								LOGGER.debug("[{},{}] chech current ({}) == next ({})", value.f0, value.f1, current, next);
								steadyState = true;
							} else {
								LOGGER.debug("[{},{}] chech, current ({}) != next ({})", value.f0, value.f1, current, next);
							}

						}
					} else {
						LOGGER.debug("[{},{}] time not elapsed: {} < {}", value.f0, value.f1, System.currentTimeMillis(), this.waitingTS);
					}
				}
			}
		}

		@Override
		public String toString() {
			return "MachineSensorData [currentTS=" + currentTS + ", waitingTS=" + waitingTS + ", steadyState=" + steadyState + ", pending="
					+ pending.size() + "]";
		}

	}

}