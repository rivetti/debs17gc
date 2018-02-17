package tech.debs17gc.system.input.soop;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public final class ObservationGroup2RDFTriplets extends RichFlatMapFunction<String, Tuple3<Integer, Long, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7058990163581832578L;
	private long currId = 0;
	private int taskId = 0;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.taskId = getRuntimeContext().getIndexOfThisSubtask();

	}

	@Override
	public void flatMap(String value, Collector<Tuple3<Integer, Long, String>> out) throws Exception {
		String[] lineOrLines = StringUtils.split("\n");

		for (int i = 0; i < lineOrLines.length; i++) {
			out.collect(new Tuple3<>(taskId, currId++, lineOrLines[i]));
		}
	}
}