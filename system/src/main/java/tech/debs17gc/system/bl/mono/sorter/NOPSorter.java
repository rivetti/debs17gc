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

public class NOPSorter
		extends RichFlatMapFunction<Tuple5<Integer, Integer, Long, Integer, Double>, Tuple4<Integer, Integer, Integer, Double>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1707778057249451058L;

	@Override
	public void flatMap(Tuple5<Integer, Integer, Long, Integer, Double> value, Collector<Tuple4<Integer, Integer, Integer, Double>> out)
			throws Exception {

		Tuple4<Integer, Integer, Integer, Double> tuple = new Tuple4<>(value.f0, value.f1, value.f3, value.f4);
		out.collect(tuple);

	}

}