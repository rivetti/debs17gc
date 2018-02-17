package tech.debs17gc.system.bl.opt;

import java.util.HashMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import tech.debs17gc.common.configuration.Config;
import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.system.bl.opt.markov.MarkovKMean;
import tech.debs17gc.system.bl.opt.sort.SortedWindow;
import tech.debs17gc.system.bl.opt.sort.SortedWindow.TerminationException;
import tech.debs17gc.system.bl.opt.sort.WindowCircularBuffer;

public class BusinessLogic
		extends RichFlatMapFunction<Tuple5<Integer, Integer, Long, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9005280474766638479L;

	private static final Logger LOGGER = LoggerFactory.getLogger(BusinessLogic.class);

	private HashMap<Integer, HashMap<Integer, SortedWindow>> data;
	private MarkovKMean logic;

	/*
	 * PERF
	 */
	private double step = 5000;

	private double countAll = 0;
	private long delayAll = 0;
	private long totalDelayAll = 0;

	private double countMKM = 0;
	private long delayMKM = 0;
	private long totalDelayMKM = 0;

	private int windowSize;

	private int terminated = 0;

	private int dataSize = 0;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		LOGGER.info("thread: {} ({})", Thread.currentThread().getName(), Thread.currentThread().getId());
		Config.load();
		MetaData.load();
		this.data = new HashMap<>(MetaData.getMachineSize());
		this.logic = new MarkovKMean(Config.getMaxIterations(), Config.getWindowSize(), Config.getConvergenceThreshold(),
				Config.getTransitionsNum());
		this.windowSize = Config.getWindowSize();
	}

	private SortedWindow create(int machine, int sensor) {
		dataSize++;
		return new SortedWindow(machine, sensor);
	}

	@Override
	public void flatMap(Tuple5<Integer, Integer, Long, Integer, Double> value,
			Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> out) throws Exception {

		int machine = value.f0;
		int sensor = value.f1;
		int ts = value.f3;

		// Handling termination for unseen machines
		if (ts < 0 && (!this.data.containsKey(machine) || !this.data.get(machine).containsKey(sensor))) {
			LOGGER.debug("[{},{}][{}] first termination for unkown machine : {}", machine, sensor, ts, value);
			return;
		}

		MDC.put("tupleKey", "p");
		if (countAll == 0) {
			LOGGER.info("first tuple at {}", System.nanoTime());
		} else if (countAll > 0 && countAll % step == 0) {
			totalDelayAll += delayAll / step;
			LOGGER.info("all count: {}, delay: {}, total: {}, avg: {}, totalAvg: {}", countAll, delayAll, totalDelayAll, delayAll / step,
					totalDelayAll * step / countAll);
			delayAll = 0;
		}

		if (countMKM > 0 && countMKM % step == 0) {
			totalDelayMKM += delayMKM / step;
			LOGGER.info("mkm count: {}, delay: {}, total: {}, avg: {}, totalAvg: {}", countMKM, delayMKM, totalDelayMKM, delayMKM / step,
					totalDelayMKM * step / countMKM);
			delayMKM = 0;
		}

		MDC.put("tupleKey", String.format("[%03d,%02d]", machine, sensor));
		long start = System.nanoTime();
		LOGGER.debug("[{},{}][{}*] new tuple {}", machine, sensor, ts, value);
		this.data.putIfAbsent(machine, new HashMap<>(MetaData.getPerMachineSensorNum()));
		this.data.get(machine).putIfAbsent(sensor, create(machine, sensor));
		SortedWindow data = this.data.get(machine).get(sensor);

		try {
			WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> window = data.update(value);
			while (window != null) {
				ts = window.getLatest().f3;
				LOGGER.debug("[{},{}][{}] window {}", machine, sensor, ts, window);
				if (window.size() == this.windowSize) {
					long startMKM = System.nanoTime();
					Tuple5<Integer, Integer, Integer, Integer, Double> output = this.logic.apply(data.machine, data.sensor, data.k, window);
					LOGGER.debug("[{},{}][{}] output: {}", machine, sensor, ts, output);
					out.collect(output);
					countMKM++;
					delayMKM += (System.nanoTime() - startMKM);
				}
				window = data.next();
			}

		} catch (TerminationException e) {
			terminated++;
			if (terminated % 10 == 0) {
				LOGGER.info("last tuple at {}", System.nanoTime());
				LOGGER.info("count: {}", countAll);
			}
			Tuple5<Integer, Integer, Integer, Integer, Double> tuple = new Tuple5<>(e.tuple.f0, e.tuple.f1, e.tuple.f3, -1, e.tuple.f4);
			out.collect(tuple);
		}
		delayAll += (System.nanoTime() - start);
		countAll++;
	}

}
