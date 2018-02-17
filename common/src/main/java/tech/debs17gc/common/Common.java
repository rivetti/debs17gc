package tech.debs17gc.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;

public class Common {
	public static final String TERMINATION_MESSAGE = "~~Termination Message~~";
	public static final byte[] TERMINATION_MESSAGE_BYTES = TERMINATION_MESSAGE.getBytes();
	// public static final String DATA_INPUT_QUEUE = getInputQueueName();// toPlatformQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME);
	// public static final String DATA_OUTPUT_QUEUE = getOutputQueueName();// toPlatformQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME);
	// public static final String RMQ_HOST = getRabbitMQHost();

	private static String toPlatformQueueName(String queueName) {
		return queueName + "." + getHobbitSessionIdKey();
	}

	private static String getHobbitSessionIdKey() {
		String key = System.getenv().get(Constants.HOBBIT_SESSION_ID_KEY);
		if (key == null) {
			key = "mySessionId";
		}
		return key;
	}

	public static String getInputQueueName() {
		return toPlatformQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME);
	}

	public static String getOutputQueueName() {
		return toPlatformQueueName(Constants.SYSTEM_2_EVAL_STORAGE_QUEUE_NAME);
	}

	public static String getRabbitMQHost() {
		String host = System.getenv().get(Constants.RABBIT_MQ_HOST_NAME_KEY);
		if (host == null) {
			host = "172.17.0.2";
		}
		return host;

	}

	public static SimpleDateFormat TS_FORMAT = new SimpleDateFormat("y.MM.dd'T'HH:mm:ss");

	public static final byte[] DIGITS = new byte[10];
	static {
		DIGITS[0] = "0".getBytes()[0];
		DIGITS[1] = "1".getBytes()[0];
		DIGITS[2] = "2".getBytes()[0];
		DIGITS[3] = "3".getBytes()[0];
		DIGITS[4] = "4".getBytes()[0];
		DIGITS[5] = "5".getBytes()[0];
		DIGITS[6] = "6".getBytes()[0];
		DIGITS[7] = "7".getBytes()[0];
		DIGITS[8] = "8".getBytes()[0];
		DIGITS[9] = "9".getBytes()[0];
	}

	public static final byte NEW_LINE = "\n".getBytes()[0];
	public static final byte GREATER_THAN = ">".getBytes()[0];
	public static final byte UNDERSCORE = "_".getBytes()[0];
	public static final byte DOT = ".".getBytes()[0];
	public static final byte QUOTE = (byte) 34;

	public static String returnLogs() {
		StringBuilder builder = new StringBuilder();
		try {
			try (Stream<Path> paths = Files.walk(Paths.get("/usr/local/flink/log/"))) {
				for (Iterator<Path> iterator = paths.iterator(); iterator.hasNext();) {
					Path filePath = (Path) iterator.next();

					builder.append(filePath.toString());
					builder.append("@#$");
					if (Files.isRegularFile(filePath)) {

						List<String> lines = Files.readAllLines(filePath);

						for (String string : lines) {
							builder.append(string);
							builder.append("@#$");
						}

					}
				}
			}
		} catch (IOException e) {
			builder.append(e.toString());
			e.printStackTrace();
		}
		return builder.toString();
	}

}
