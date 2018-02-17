package tech.debs17gc.adapter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.Utils;
import tech.debs17gc.common.configuration.Config;

public class TechDetectorAdapterMC extends AbstractCommandReceivingComponent {
	private static final Logger LOGGER = LoggerFactory.getLogger(TechDetectorAdapterMC.class);

	// private static final String GUEST_HOST_NAME_ENV = "GUEST HOST_NAME_ENV";

	private static final String MASTER_HOST_IP_ENV = "MASTER_HOST_IP_ENV";
	private static final String MASTER_CONTAINER_NAME_ENV = "MASTER_CONTAINER_NAME_ENV";

	private static final String DOCKER_IMAGE_NAME = "git.project-hobbit.eu:4567/nrivetti/techdetector";
	private static final String DOCKER_IMAGE_TYPE = "system";

	private final int slaveCount;
	private final ArrayList<String> slaves;

	private final String hostAddress;
	private final String containerName;
	private final String masterContainerName;
	private final String masterHostName;

	public static void main(String... args) throws Exception {
		TechDetectorAdapterMC system = null;
		try {
			system = new TechDetectorAdapterMC();
			system.init();
			system.run();
		} finally {
			if (system != null) {
				system.close();
			}
		}
	}

	public TechDetectorAdapterMC() throws UnknownHostException {
		LOGGER.info("Initialize Adapter");
		Config.load();

		String hname = InetAddress.getLocalHost().getHostName();
		InetAddress[] addresses = InetAddress.getAllByName(hname);
		this.hostAddress = addresses[addresses.length - 1].getHostAddress();
		LOGGER.info("InetAddress.getLocalHost().getHostAddress(): {}", InetAddress.getLocalHost().getHostAddress());
		LOGGER.info("InetAddress.getLocalHost().getHostAddress(): {}", Arrays.toString(InetAddress.getAllByName(this.hostAddress)));

		// 172.17.2.11/

		this.containerName = System.getenv(Constants.CONTAINER_NAME_KEY);

		this.masterContainerName = System.getenv(MASTER_CONTAINER_NAME_ENV);
		this.masterHostName = System.getenv(MASTER_HOST_IP_ENV);

		this.slaveCount = Config.getSlaveNum();
		this.slaves = new ArrayList<>(this.slaveCount);
	}

	@Override
	public void init() throws Exception {
		LOGGER.info("Initializing Hobbit...");
		super.init();
		String hobbitSessionId = getHobbitSessionId();
		if (hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS)
				|| hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS)) {
			throw new IllegalStateException(
					"Wrong hobbit session id. It must not be equal to HOBBIT_SESSION_ID_FOR_BROADCASTS or HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS");
		}
	}

	@Override
	public void run() throws Exception {

		// parseParamsFromEnv();
		// LOGGER.info("[{},{}] guest hostname is {}", this.containerName, this.hostname, System.getenv().get(GUEST_HOST_NAME_ENV));

		// I am Master
		if (this.masterContainerName == null) {

			LOGGER.info("[{},{}] is master", this.containerName, this.hostAddress);
			getIPInfo();

			String encodedModel = System.getenv().get(Constants.SYSTEM_PARAMETERS_MODEL_KEY); // read environment variable
			LOGGER.info("Encoded model {}", encodedModel);

			// getCpuInfo();
			// getMemInfo();

			LOGGER.info("[{},{}] Configuring Flink on master", this.containerName, this.hostAddress);
			setJobManagerHostname(this.hostAddress);
			setTaskManagerHostname(this.hostAddress);
			setTaskManagerSlots(Config.getMasterSlotNum());

			LOGGER.info("[{},{}] Starting Job Manager on master", this.containerName, this.hostAddress);
			executeCommandWaiting("/usr/local/flink/bin/jobmanager.sh start cluster");

			Thread.sleep(5000);

			LOGGER.info("[{},{}] Starting Task Manager on master", this.containerName, this.hostAddress);
			executeCommandWaiting("/usr/local/flink/bin/taskmanager.sh start");
			Thread.sleep(5000);
			// TODO how to check jobmanager is running?

			for (int i = 0; i < slaveCount; i++) {
				String slaveName = createSlaveContainer();
				if (slaveName != null)
					this.slaves.add(slaveName);
				Thread.sleep(5000);
			}
			Thread.sleep(5000 * (slaves.size() + 1));
			// TODO how to check that all taskmanager are connected?

			LOGGER.info("[{},{}] Starting Job", this.containerName, this.hostAddress);
			executeCommand(
					"/usr/local/flink/bin/flink run -c tech.debs17gc.system.topology.SimpleTopology  /root/workdir/debs17gc/system-0.0.1-SNAPSHOT.jar &");
			Thread.sleep(10000 * (slaves.size() + 6));
			// TODO how to check that the job is running?

			LOGGER.info("[{},{}] Sending SYSTEM_READY_SIGNAL...", this.containerName, this.hostAddress);
			sendToCmdQueue(Commands.SYSTEM_READY_SIGNAL);
			LOGGER.info("[{},{}] SYSTEM READY SIGNAL sent", this.containerName, this.hostAddress);

			// I am Slave
		} else {
			LOGGER.info("[{},{}] is slave of {},{}", this.containerName, this.hostAddress, this.masterContainerName, this.masterHostName);
			// getCpuInfo();
			// getMemInfo();
			getIPInfo();

			LOGGER.info("[{},{}] Configuring Flink on slave", this.containerName, this.hostAddress);
			setJobManagerHostname(this.masterHostName);
			setTaskManagerHostname(this.hostAddress);
			setTaskManagerSlots(Config.getSlaveSlotNum());
			Thread.sleep(1000);
			LOGGER.info("[{},{}] Starting Task Manager on slave", this.containerName, this.hostAddress);
			executeCommandWaiting("/usr/local/flink/bin/taskmanager.sh start");

		}

	}

	@Override
	public void receiveCommand(byte command, byte[] data) {
		LOGGER.info("[{},{}] received hobbit command {}", this.containerName, this.hostAddress, command);
	}

	private void executeCommand(String command) {

		try {
			Runtime.getRuntime().exec(command);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private String createSlaveContainer() {
		String[] env = new String[3];
		env[0] = String.format("%s=%s", MASTER_HOST_IP_ENV, this.hostAddress);
		env[1] = String.format("%s=%s", MASTER_CONTAINER_NAME_ENV, this.containerName);
		env[2] = String.format("%s=%s", Constants.SYSTEM_PARAMETERS_MODEL_KEY, System.getenv().get(Constants.SYSTEM_PARAMETERS_MODEL_KEY));
		LOGGER.info("[{},{}] master creating new container", this.containerName, this.hostAddress);
		String newContainer = createContainer(DOCKER_IMAGE_NAME, DOCKER_IMAGE_TYPE, env);
		LOGGER.info("[{},{}] master created new container {}", this.containerName, this.hostAddress, newContainer);
		return newContainer;
	}

	/*
	 * private void setSlaves() { LOGGER.info("[{},{}] adding flink slaves", this.containerName, this.hostAddress); try (PrintWriter writer
	 * = new PrintWriter(new File("/usr/local/flink/conf/slaves"))) { LOGGER.info("[{},{}] adding flink master host {} to slaves",
	 * this.containerName, this.hostAddress, this.hostAddress); writer.println(this.hostAddress); for (String slave : slaves) {
	 * LOGGER.info("[{},{}] adding flink slave host {} to slaves", this.containerName, this.hostAddress, slave); writer.println(slave); } }
	 * catch (FileNotFoundException e) { // TODO Auto-generated catch block e.printStackTrace(); } }
	 */

	private void setTaskManagerHostname(String hostname) {
		executeCommandWaiting(String.format("sed -i s/%%taskmanager%%/%s/g /usr/local/flink/conf/flink-conf.yaml", hostname));
	}

	private void setJobManagerHostname(String hostname) {
		executeCommandWaiting(String.format("sed -i s/%%jobmanager%%/%s/g /usr/local/flink/conf/flink-conf.yaml", hostname));
	}

	private void setTaskManagerSlots(int slots) {
		executeCommandWaiting(String.format("sed -i s/%%taskslots%%/%d/g /usr/local/flink/conf/flink-conf.yaml", slots));
	}

	private void getIPInfo() {
		executeCommandWaiting("ip addr");
	}

	private void getMemInfo() {
		executeCommandWaiting("cat /proc/meminfo");
	}

	private void getCpuInfo() {
		executeCommandWaiting("cat /proc/cpuinfo");
	}

	private void executeCommandWaiting(String command) {

		Process p;
		try {
			LOGGER.info("[{},{}] executing cmd {}", this.containerName, this.hostAddress, command);
			p = Runtime.getRuntime().exec(command);
			LOGGER.info("[{},{}] cmd {} returned code {}", this.containerName, this.hostAddress, command, p.waitFor());

			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				LOGGER.info("[{},{}] cmd {} out: {}", this.containerName, this.hostAddress, command, line);
			}

			reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

			while ((line = reader.readLine()) != null) {
				LOGGER.info("[{},{}] cmd {} err: {}", this.containerName, this.hostAddress, command, line);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}