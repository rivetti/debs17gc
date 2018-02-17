package tech.debs17gc.system.output.bytearray;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Common;
import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;

public class ByteArraySerializer2 extends RichFlatMapFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, byte[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4651342380326188830L;

	private static final Logger LOGGER = LoggerFactory.getLogger(ByteArraySerializer2.class);
	private HashMap<Integer, TerminationData> termination;
	private TreeMap<Integer, TimeStampData> pending;
	private int anomaliesCount = 0;
	private int nextTS = -1;

	private int transitionNum = 0;
	private double probabilityThreshold = 0;
	private int machineSensorNum = 0;

	private int countTuples = 0;

	/*
	 * PERF
	 */
	private double count = 0;
	private double step = 100000;
	private long delay = 0;
	private long totalDelay = 0;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		LOGGER.info("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		Config.load();
		MetaData.load();

		this.pending = new TreeMap<>();
		this.termination = new HashMap<>();
		this.anomaliesCount = 0;
		this.nextTS = 0;

		this.probabilityThreshold = Config.getProbabilityThreshold();
		this.machineSensorNum = MetaData.getPerMachineSensorNum();
		
		this.transitionNum = Config.getTransitionsNum();

		this.countTuples = 0;
	}

	@Override
	// MID, TS, SID, VALUE
	public void flatMap(Tuple5<Integer, Integer, Integer, Integer, Double> value, Collector<byte[]> out) throws Exception {
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

		if (value.f2 == -1) {
			if (termination.containsKey(value.f0)) {
				termination.get(value.f0).terminatedSensor.add(value.f1);
				LOGGER.debug("Terminated Sensor ({},{})", value.f0, value.f1);
				if (termination.get(value.f0).terminatedSensor.size() == termination.get(value.f0).sensorNum) {
					termination.remove(value.f0);
					LOGGER.info("Terminated Machine {}", value.f0);
				}
				if (termination.isEmpty()) {
					if (LOGGER.isDebugEnabled()) {
						// TODO debug
						for (Iterator<Entry<Integer, TimeStampData>> iterator = pending.entrySet().iterator(); iterator.hasNext();) {
							Entry<Integer, TimeStampData> entry = (Entry<Integer, TimeStampData>) iterator.next();
							LOGGER.warn("Still Pending {} -> {}", entry.getKey(), entry.getValue());

						}
					}
					LOGGER.info("last tuple at {}", System.nanoTime());
					LOGGER.info("count: {}", countTuples);
					LOGGER.info("Sending Termination message {}", Common.TERMINATION_MESSAGE);
					out.collect(Common.TERMINATION_MESSAGE_BYTES);
				}

			} else {
				LOGGER.warn("termination for unkown machine : {}", value);
			}
			return;

		} else if (value.f2 >= nextTS)

		{
			this.countTuples++;

			termination.putIfAbsent(value.f0, new TerminationData(value.f0));

			// Has to wait, adding it in the pending queue
			pending.putIfAbsent(value.f2, new TimeStampData(value));
			pending.get(value.f2).add(value);
			LOGGER.debug("Added {} with TS {} to {}", value, value.f2, pending.get(value.f2));

			Iterator<Entry<Integer, TimeStampData>> it = pending.entrySet().iterator();

			while (it.hasNext()) {
				Entry<Integer, TimeStampData> next = it.next();
				if (nextTS == next.getKey()) {
					LOGGER.trace("NextTS ({}) equal to next pending TS ({} -> {})", nextTS, next.getValue(), next.getKey());
					if (next.getValue().count == 0) {
						LOGGER.trace("current pending TS ({} -> {}) finished", nextTS, next.getValue(), next.getKey());
						// TODO What happens when several anomalies are detected for the same TS? (ie same machine in same TS on different
						// sensors => sort on sensor ID
						for (Tuple5<Integer, Integer, Integer, Integer, Double> pendingValue : next.getValue().tuples.values()) {
							// TODO for debug, to be pushed again in TimeStampData class
							if (isAnomaly(pendingValue)) {
								LOGGER.debug("Anomaly {}: {}", anomaliesCount, pendingValue);
								out.collect(ByteArraySerializerUtils.serialize(anomaliesCount, pendingValue, transitionNum));
								anomaliesCount++;
							} else {
								LOGGER.debug("Not Anomaly: {}", pendingValue);
							}
						}
						it.remove();
						nextTS++;
					} else {
						LOGGER.trace("current pending TS ({} -> {}) not finished", nextTS, next.getValue(), next.getKey());
						break;
					}
				} else {
					LOGGER.trace("NextTS ({}) smaller than next pending TS ({} -> {})", nextTS, next.getValue(), next.getKey());
					// TODO Could not find an at-least-as-efficient version without break
					break;
				}
			}
		}

		delay += System.nanoTime() - start;

	}

	private boolean isAnomaly(Tuple5<Integer, Integer, Integer, Integer, Double> value) {
		// TODO organizers reply is <
		return value.f4 < this.probabilityThreshold;
	}

	public class TerminationData {
		public final int sensorNum;
		// TODO change into counter?
		public HashSet<Integer> terminatedSensor;

		public TerminationData(int machine) {
			super();
			this.sensorNum = ByteArraySerializer2.this.machineSensorNum;
			this.terminatedSensor = new HashSet<>(this.sensorNum);
		}

	}

	public class TimeStampData {
		public int machine;
		public int count;

		public TreeMap<Integer, Tuple5<Integer, Integer, Integer, Integer, Double>> tuples;

		public TimeStampData(Tuple5<Integer, Integer, Integer, Integer, Double> value) {
			this.machine = value.f0;
			this.count = ByteArraySerializer2.this.machineSensorNum;
			this.tuples = new TreeMap<>();
		}

		public void add(Tuple5<Integer, Integer, Integer, Integer, Double> tuple) {
			if (LOGGER.isDebugEnabled() || isAnomaly(tuple)) {// || tuple.f1 == 66 || tuple.f1 == 76) {
				tuples.put(tuple.f1, tuple);
			}
			count--;
		}

		@Override
		public String toString() {
			return "TimeStampData [machine=" + machine + ", count=" + count + ", tuples=" + tuples.size() + "]";
		}

	}

}
