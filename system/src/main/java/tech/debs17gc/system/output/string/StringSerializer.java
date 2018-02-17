package tech.debs17gc.system.output.string;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.common.metadata.MetaData.Machine;
import tech.debs17gc.common.parsing.SerializeInteger;

public class StringSerializer extends RichFlatMapFunction<Tuple5<Integer, Integer, Integer, Double, Double>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4651342380326188830L;

	private TreeMap<Integer, List<Tuple5<Integer, Integer, Integer, Double, Double>>> pending;
	private int anomaliesCount = 0;
	private int nextTS = -1;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		if (this.getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
			throw new RuntimeException();
		}

		this.pending = new TreeMap<>();
		this.anomaliesCount = 0;
		this.nextTS = 0;
	}

	@Override
	// MID, TS, SID, VALUE
	public void flatMap(Tuple5<Integer, Integer, Integer, Double, Double> value, Collector<String> out) throws Exception {
		// check TS
		if (value.f1 < nextTS) {
			// TODO safety
			throw new RuntimeException();
		} else if (value.f1 > nextTS) {
			// Has to wait, adding it in the pending queue
			pending.putIfAbsent(value.f1, new ArrayList<>());

			if (isAnomaly(value)) {
				pending.get(value.f1).add(value);
			}
		} else {
			if (isAnomaly(value)) {
				out.collect(serialize(anomaliesCount, value));
				anomaliesCount++;
			}
			nextTS++;
			Iterator<Entry<Integer, List<Tuple5<Integer, Integer, Integer, Double, Double>>>> it = pending.entrySet().iterator();

			while (it.hasNext()) {
				Entry<Integer, List<Tuple5<Integer, Integer, Integer, Double, Double>>> next = it.next();
				if (nextTS == next.getKey()) {
					// TODO What happens when several anomalies are detected for the same TS? (ie same machine in same TS on different
					// sensors
					for (Tuple5<Integer, Integer, Integer, Double, Double> pendingValue : next.getValue()) {
						out.collect(serialize(anomaliesCount, pendingValue));
						anomaliesCount++;
					}
					it.remove();
					nextTS++;
				} else {
					// TODO Could not find an at-least-as-efficient version without break
					break;
				}
			}

		}
	}

	private boolean isAnomaly(Tuple5<Integer, Integer, Integer, Double, Double> value) {
		double threshold = MetaData.getMachine(value.f0).getSensor(value.f2).pt;
		// TODO strict?
		return value.f4 < threshold;
	}

	private static String serialize(int anomaliesCount, Tuple5<Integer, Integer, Integer, Double, Double> value) {
		//@formatter:off
		String output = String.format(OUTPUT, anomaliesCount, 
				              anomaliesCount, value.f0, 
				              anomaliesCount, value.f0, value.f2,
				              anomaliesCount, value.f1,
				              anomaliesCount, value.f4);
		//@formatter:on

		return output;
	}

	private final static String LEADING = "<http://project-hobbit.eu/resources/debs2017#Anomaly_";
	private final static String TRAILING = "> .\n";

	private final static String HEADER_TRAILING = "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> .\n";

	private final static String MACHINE_MIDST = "> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_";

	private final static String SENSOR_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_";

	private final static String TS_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimeStamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_";

	private final static String VALUE_MIDST = "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> \"";
	private final static String VALUE_TRAILING = "\"^^<http://www.w3.org/2001/XMLSchema#double> .\n";

	//@formatter:off
	private final static String OUTPUT = LEADING + "%d" + HEADER_TRAILING + 
			                             LEADING + "%d" + MACHINE_MIDST + "%d" + TRAILING +
                                         LEADING + "%d" + SENSOR_MIDST + "%d_%d" + TRAILING +
                                         LEADING + "%d" + TS_MIDST + "%d" + TRAILING +
                                         LEADING + "%d" + VALUE_MIDST + "%f" + VALUE_TRAILING ;	
	//@formatter:on

	//@formatter:off
    //<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_59> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_59_31> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimeStamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_24> .
	//<http://project-hobbit.eu/resources/debs2017#Anomaly_0> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> "0.004115226337448559"^^<http://www.w3.org/2001/XMLSchema#double> .
	//@formatter:on
}
