package tech.debs17gc.common.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
	private static Properties props = new Properties();

	/*
	 * System Params
	 */

	public static final String TOPOLOGY_CLASS = "topology.class";
	public static final String METADATA_PATH = "metadata.path";

	public static String getTopologyClass() {
		return props.getProperty(TOPOLOGY_CLASS);
	}

	public static String getMetaDataPath() {
		return props.getProperty(METADATA_PATH);
	}

	/*
	 * Flink Params
	 */

	public static final String SLAVE_NUM = "slave.num";
	public static final String SLAVE_TASK_MANAGER_NUM = "slave.taskmanager.num";
	public static final String SLAVE_SLOT_NUM = "slave.slot.num";
	public static final String MASTER_TASK_MANAGER_NUM = "master.taskmanager.num";
	public static final String MASTER_SLOT_NUM = "master.slot.num";

	public static int getSlaveNum() {
		return getInteger(SLAVE_NUM);
	}

	public static int getSlaveTaskManagerNum() {
		return getInteger(SLAVE_TASK_MANAGER_NUM);
	}

	public static int getSlaveSlotNum() {
		return getInteger(SLAVE_SLOT_NUM);
	}

	public static int getMasterTaskManagerNum() {
		return getInteger(MASTER_TASK_MANAGER_NUM);
	}

	public static int getMasterSlotNum() {
		return getInteger(MASTER_SLOT_NUM);
	}
	
	/*
	 * Rabbit MQ params
	 */
	
	public static final String QOS_PREFETCH = "rabbitmq.qos.prefetch";
	
	public static int getQoSPrefetch(){
		return getInteger(QOS_PREFETCH);
	}
	

	/*
	 * Application Params
	 */
	public static final String WINDOW_SIZE = "window.size";
	public static final String MAX_ITERATIONS = "iterations.max";
	public static final String TRANSITIONS = "transitions.num";
	public static final String CONVERGENCE_THRESHOLD = "convergence.threshold";
	public static final String PRECISION = "precision";
	public static final String PROBABILITY_THRESHOLD = "probability.treshold";

	public static int getWindowSize() {
		return getInteger(WINDOW_SIZE);
	}

	public static int getMaxIterations() {
		return getInteger(MAX_ITERATIONS);
	}

	public static int getTransitionsNum() {
		return getInteger(TRANSITIONS);
	}

	public static double getConvergenceThreshold() {
		return getDouble(CONVERGENCE_THRESHOLD);
	}

	public static int getPrecision() {
		return getInteger(PRECISION);
	}

	public static double getProbabilityThreshold() {
		return getDouble(PROBABILITY_THRESHOLD);
	}

	/*
	 * Parallelism Params
	 */

	public static final String SOURCE_PARALLLELISM = "source.parallelism";
	public static final String PARSER_PARALLLELISM = "parser.parallelism";
	public static final String SORTER_PARALLLELISM = "sorter.parallelism";
	public static final String BL_PARALLLELISM = "bl.parallelism";
	public static final String SERIALIZER_PARALLLELISM = "serializer.parallelism";
	public static final String SINK_PARALLLELISM = "sink.parallelism";

	public static int getSourceParallelism() {
		return getInteger(SOURCE_PARALLLELISM);
	}

	public static int getParserParallelism() {
		return getInteger(PARSER_PARALLLELISM);
	}

	public static int getSorterParallelism() {
		return getInteger(SORTER_PARALLLELISM);
	}

	public static int getBLParallelism() {
		return getInteger(BL_PARALLLELISM);
	}

	public static int getSerializerParallelism() {
		return getInteger(SERIALIZER_PARALLLELISM);
	}

	public static int getSinkParallelism() {
		return getInteger(SINK_PARALLLELISM);
	}

	/*
	 * Chaining Params
	 */

	public static final String PARSER_CHAINING = "parser.chaining";
	public static final String SORTER_CHAINING = "sorter.chaining";
	public static final String BL_CHAINING = "bl.chaining";
	public static final String SERIALIZER_CHAINING = "serializer.chaining";
	public static final String SINK_CHAINING = "sink.chaining";

	public static <T> SingleOutputStreamOperator<T> parserChaining(SingleOutputStreamOperator<T> stream) {
		return chaining(stream, PARSER_CHAINING);
	}

	public static <T> SingleOutputStreamOperator<T> sorterChaining(SingleOutputStreamOperator<T> stream) {
		return chaining(stream, SORTER_CHAINING);
	}

	public static <T> SingleOutputStreamOperator<T> blChaining(SingleOutputStreamOperator<T> stream) {
		return chaining(stream, BL_CHAINING);
	}

	public static <T> SingleOutputStreamOperator<T> serializerChaining(SingleOutputStreamOperator<T> stream) {
		return chaining(stream, SERIALIZER_CHAINING);
	}

	/*
	 * Resource Group Params
	 */

	public static final String SOURCE_GROUP = "source.group";
	public static final String PARSER_GROUP = "parser.group";
	public static final String SORTER_GROUP = "sorter.group";
	public static final String BL_GROUP = "bl.group";
	public static final String SERIALIZER_GROUP = "serializer.group";
	public static final String SINK_GROUP = "sink.group";

	public static String getSourceGroup() {
		return props.getProperty(SOURCE_GROUP, "default");
	}

	public static String getParserGroup() {
		return props.getProperty(PARSER_GROUP, "default");
	}

	public static String getSorterGroup() {
		return props.getProperty(SORTER_GROUP, "default");
	}

	public static String getBLGroup() {
		return props.getProperty(BL_GROUP, "default");
	}

	public static String getSerializerGroup() {
		return props.getProperty(SERIALIZER_GROUP, "default");
	}

	public static String getSinkGroup() {
		return props.getProperty(SINK_GROUP, "default");
	}
	
	/*
	 * Timeout Params
	 */


	public static final String SOURCE_TIMEOUT = "source.timeout";
	public static final String PARSER_TIMEOUT = "parser.timeout";
	public static final String SORTER_TIMEOUT = "sorter.timeout";
	public static final String BL_TIMEOUT = "bl.timeout";
	public static final String SERIALIZER_TIMEOUT = "serializer.timeout";
	
	public static int getSourceTimeout() {
		return getInteger(SOURCE_TIMEOUT);
	}

	public static int getParserTimeout() {
		return getInteger(PARSER_TIMEOUT);
	}

	public static int getSorterTimeout() {
		return getInteger(SORTER_TIMEOUT);
	}

	public static int getBLTimeout() {
		return getInteger(BL_TIMEOUT);
	}

	public static int getSerializerTimeout() {
		return getInteger(SERIALIZER_TIMEOUT);
	}
	
	/*
	 * Out of Order Params
	 */
	public static final String INITIALDELAY = "initial.delay"; // 500;
	public static final String ENDINGDELAY = "ending.delay"; // 5000;
	public static final String INTERARRIVALDELAY = "interArrival.delay"; // 1000;
	public static final String MINSEQUENCELENGHT = "min.sequence.length"; // 20;

	public static long getInitialDelay() {
		return getLong(INITIALDELAY);
	}

	public static long getEndingDelay() {
		return getLong(ENDINGDELAY);
	}

	public static long getInterArrivalDelay() {
		return getLong(INTERARRIVALDELAY);
	}

	public static int getMinSequenceLength() {
		return getInteger(MINSEQUENCELENGHT);
	}

	/*
	 * Loading
	 */

	public synchronized static void load() {
		if (!props.isEmpty())
			return;
		getConfigFromSystemParams();
		if (!props.isEmpty())
			return;
		String path = new File(System.getProperty("user.home") + "/workdir/debs17gc/data/config.prop").getPath();
		if (path == null) {
			path = new File("/root/workdir/debs17gc/data/config.prop").getPath();
		}
		load(path);
	}

	public synchronized static void load(String path) {
		if (!props.isEmpty())
			return;

		try {
			FileInputStream fis = new FileInputStream(path);
			props.load(fis);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static String PARAMS_LEADING = "http://www.techdetector.org/gc/";

	private static void getConfigFromSystemParams() {
		try {

			String encodedModel = System.getenv().get(Constants.SYSTEM_PARAMETERS_MODEL_KEY); // read environment variable
			LOGGER.debug("Encoded model {}", encodedModel);
			if (encodedModel != null) {
				Model model = RabbitMQUtils.readModel(encodedModel); // decode Jena model
				LOGGER.info("Reading System Paremters Model");
				/*
				 * for (StmtIterator iterator = model.listStatements(null, null, (RDFNode) null); iterator.hasNext();) { Statement next =
				 * iterator.next();
				 * 
				 * Resource subject = next.getSubject(); // get the subject Property predicate = next.getPredicate(); // get the predicate
				 * RDFNode object = next.getObject(); // get the object LOGGER.info("s: {}, p: {}, o: {}", subject, predicate, object); }
				 */
				for (StmtIterator iterator = model.listStatements(null, null, (RDFNode) null); iterator.hasNext();) {
					Statement next = iterator.next();

					Property predicate = next.getPredicate(); // get the predicate
					RDFNode object = next.getObject(); // get the object
					LOGGER.debug("p: {}, o: {}", predicate, object);

					if (predicate.toString().startsWith(PARAMS_LEADING)) {
						String key = predicate.toString().substring(PARAMS_LEADING.length());
						if (object.isLiteral()) {
							String value = object.asLiteral().getString();
							LOGGER.debug("Add property to config: {}={}", key, value); // props.setProperty(key, value);
							props.put(key, value);
						} else {
							LOGGER.debug("not litertal p: {}, o: {}", predicate, object);
						}

					}

				}

			}
		} catch (Exception e) {
			LOGGER.warn("Exception", e);
		}

	}

	public static <T> DataStreamSink<T> sinkChaining(DataStreamSink<T> stream) {
		if (!Boolean.parseBoolean(props.getProperty(SINK_CHAINING))) {
			return stream.disableChaining();
		} else {
			return stream;
		}
	}

	private static <T> SingleOutputStreamOperator<T> chaining(SingleOutputStreamOperator<T> stream, String propName) {
		if (!Boolean.parseBoolean(props.getProperty(propName))) {
			return stream.startNewChain();
		} else {
			return stream;
		}
	}

	private static long getLong(String name) {
		return Long.parseLong(props.getProperty(name));
	}

	private static int getInteger(String name) {
		return Integer.parseInt(props.getProperty(name));
	}

	private static Set<Integer> getIntegers(String name) {
		HashSet<Integer> res = new HashSet<>();
		if (props.containsKey(name)) {
			String[] tokens = props.getProperty(name).split(":");

			for (String string : tokens) {
				res.add(Integer.parseInt(string));
			}
		}
		return res;
	}

	private static double getDouble(String name) {
		return Double.parseDouble(props.getProperty(name));
	}
}
