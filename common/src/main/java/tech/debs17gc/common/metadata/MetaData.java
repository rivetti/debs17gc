package tech.debs17gc.common.metadata;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.MachineTypes;
import tech.debs17gc.common.configuration.Config;

public class MetaData {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetaData.class);

	public static final int MACHINE_ID_LEADING_OFFSET = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#Machine_".length();
	public static final int MACHINE_TYPE_LEADING_OFFSET = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#".length();
	public static final int SENSOR_ID_LEADING_OFFSET = "http://www.agtinternational.com/ontologies/WeidmullerMetadata#_".length();

	private static final TreeMap<Integer, Machine> metaData = new TreeMap<>();

	private static int probabilityPrecision = 0;
	private static int totalSensorNum = 0;

	public static int getPrecision() {
		return probabilityPrecision;
	}

	public static Iterator<Entry<Integer, Machine>> getMachineIterator() {
		return metaData.entrySet().iterator();
	}

	public static int getMachineSize() {
		return metaData.size();
	}
	
	public static int getPerMachineSensorNum(){
		return metaData.firstEntry().getValue().sensorSize();
	}

	public static int getTotalSensorNum() {
		return totalSensorNum;
	}

	static {
		// MetaData.load();
	}

	public synchronized static void load() {
		if (!metaData.isEmpty())
			return;
		Config.load();
		load(Config.getMetaDataPath());
	}

	public synchronized static void load(String path) {
		if (!metaData.isEmpty())
			return;

		LOGGER.info("Load MetaData from: {}", path);
		Model model = ModelFactory.createDefaultModel();
		model.read(path);

		//@formatter:off
		final String query = 
           "prefix wmd: <http://www.agtinternational.com/ontologies/WeidmullerMetadata#> \n" +
           "prefix syntax: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
           "prefix core: <http://www.agtinternational.com/ontologies/IoTCore#> \n" +        		   
           "prefix ssn: <http://purl.oclc.org/NET/ssnx/ssn#> \n" +
           "SELECT ?machine ?type ?sensor ?k ?pt\n" + 
           "WHERE { ?sensor syntax:type             ?sType .\n" +
           "        ?sensor wmd:hasNumberOfClusters ?k .\n" +
           "        ?pts wmd:isThresholdForProperty ?sensor .\n" +              
           "        ?pts core:valueLiteral ?pt .\n" +           
              "{\n" +
              "SELECT ?machine ?type ?model ?sensor \n" + 
              "WHERE { ?model ssn:hasProperty ?sensor .\n" +		        
                 "{\n" +
                 "SELECT ?machine ?type ?model  \n"+
                 "WHERE {  ?machine  core:hasModel  ?model .\n" +
                 "         ?machine  syntax:type    ?type .\n" +      
                 "}\n"+		          
                 "}\n"+                 
              "}\n"+		          
              "}\n"+                 
           "}\n"       ;
		//@formatter:on

		final QueryExecution exec = QueryExecutionFactory.create(query, model);
		final ResultSet rs = exec.execSelect();
		double smallestProbability = Double.MAX_VALUE;
		while (rs.hasNext()) {
			final QuerySolution qs = rs.next();

			Machine machine = MetaData.addMachine(parseMachineID(qs.get("machine")), parseMachineType(qs.get("type")));

			Sensor sensor = machine.addSensor(parseSensorID(qs.get("sensor")), parseK(qs.get("k")), parsePT(qs.get("pt")));
			totalSensorNum++;
			if (sensor.pt < smallestProbability) {
				smallestProbability = sensor.pt;
			}

			LOGGER.debug(sensor.toString());

		}
		probabilityPrecision = (int) -Math.ceil(Math.log10(smallestProbability)) + 1;
		LOGGER.info("probabilityPrecision: {}", probabilityPrecision);
		LOGGER.info("MetaData loaded for {} machines and {} sensors", metaData.size(), totalSensorNum);
	}

	private static int parseMachineID(RDFNode node) {
		String num = StringUtils.substring(node.toString(), MACHINE_ID_LEADING_OFFSET);
		return Integer.parseInt(num);
	}

	private static MachineTypes parseMachineType(RDFNode node) {
		String type = StringUtils.substring(node.toString(), MACHINE_TYPE_LEADING_OFFSET, MACHINE_TYPE_LEADING_OFFSET + 1);
		switch (type) {
		case "M":
			return MachineTypes.MOLDING;

		case "A":
			return MachineTypes.ASSEMBLY;

		default:
			throw new RuntimeException(type);
		}
	}

	private static int parseSensorID(RDFNode node) {
		String num = StringUtils.substring(node.toString(), StringUtils.indexOf(node.toString(), "_", SENSOR_ID_LEADING_OFFSET) + 1);
		return Integer.parseInt(num);
	}

	private static int parseK(RDFNode node) {
		String num = StringUtils.substring(node.toString(), 0, StringUtils.indexOf(node.toString(), "^"));
		return Integer.parseInt(num);
	}

	private static double parsePT(RDFNode node) {
		String num = StringUtils.substring(node.toString(), 0, StringUtils.indexOf(node.toString(), "^"));
		return Double.parseDouble(num);
	}

	public static Machine addMachine(int id, MachineTypes type) {
		if (metaData.containsKey(id)) {
			Machine machine = metaData.get(id);
			if (machine.type != type) {
				throw new RuntimeException();
			}
			return machine;
		} else {
			Machine machine = new Machine(id, type);
			metaData.put(id, machine);
			return machine;
		}
	}

	public static boolean machineExists(int id) {
		return metaData.containsKey(id);
	}

	public static Machine getMachine(int id) {
		return metaData.get(id);
	}

	public static String print() {

		StringBuilder builder = new StringBuilder();
		for (Entry<Integer, Machine> entry : metaData.entrySet()) {

			builder.append(String.format("##### %05d #####\n%s\n", entry.getKey(), entry.getValue().print()));
		}
		builder.setLength(builder.length() - 1);
		return builder.toString();

	}

	public static String stats() {
		TreeMap<Double, Double> ptValues = new TreeMap<>();
		TreeMap<Integer, Double> kValues = new TreeMap<>();
		TreeMap<Integer, Double> sensorIDs = new TreeMap<>();
		TreeMap<Integer, Integer> numSensor = new TreeMap<>();
		TreeMap<Integer, HashSet<Integer>> perSensorKValue = new TreeMap<>();
		int sensorCount = 0;
		for (Machine machine : metaData.values()) {
			for (Sensor sensor : machine.sensors.values()) {
				ptValues.compute(sensor.pt, (k, v) -> {
					if (v != null) {
						return v + 1;
					} else {
						return 1.0;
					}
				});

				kValues.compute(sensor.k, (k, v) -> {
					if (v != null) {
						return v + 1;
					} else {
						return 1.0;
					}
				});

				sensorIDs.compute(sensor.id, (k, v) -> {
					if (v != null) {
						return v + 1;
					} else {
						return 1.0;
					}
				});

				perSensorKValue.putIfAbsent(sensor.id, new HashSet<>());
				perSensorKValue.get(sensor.id).add(sensor.k);

				sensorCount++;

			}
			numSensor.put(machine.machine, machine.sensors.size());
		}

		{
			final double count = sensorCount;
			ptValues.replaceAll((k, v) -> {
				return v / (double) count;
			});

			kValues.replaceAll((k, v) -> {
				return v / (double) count;
			});

			sensorIDs.replaceAll((k, v) -> {
				return v / (double) count;
			});

		}
		double mean = 0;
		for (Entry<Integer, Double> entry : kValues.entrySet()) {
			mean += entry.getValue() * (double) entry.getKey();
		}

		StringBuilder builder = new StringBuilder();
		builder.append(ptValues.toString());
		builder.append("\n");
		builder.append(kValues.toString());
		builder.append("\n");
		builder.append(Collections.max(kValues.values()));
		builder.append("\n");
		builder.append(Collections.min(kValues.values()));
		builder.append("\n");
		builder.append(mean);
		builder.append("\n");
		builder.append(sensorIDs.toString());
		builder.append("\n");
		builder.append(Collections.max(sensorIDs.values()));
		builder.append("\n");
		builder.append(Collections.min(sensorIDs.values()));
		builder.append("\n");
		builder.append(numSensor.toString());
		builder.append("\n");
		builder.append(sensorCount);
		builder.append("\n");
		builder.append(perSensorKValue.toString());
		return builder.toString();
	}

	public static class Machine {

		public final int machine;
		public final byte[] byteMachine;
		public final MachineTypes type;

		private final TreeMap<Integer, Sensor> sensors;

		private Machine(int machine, MachineTypes type) {
			super();
			this.machine = machine;
			this.byteMachine = String.format("%d", machine).getBytes();
			this.type = type;
			this.sensors = new TreeMap<>();
		}

		public int sensorSize() {
			return this.sensors.size();
		}

		public Sensor addSensor(int id, int k, double pt) {
			if (sensors.containsKey(id)) {
				Sensor sensor = sensors.get(id);
				if (sensor.k != k || sensor.pt != pt) {
					throw new RuntimeException();
				}
				return sensor;

			} else {
				Sensor sensor = new Sensor(machine, id, k, pt);
				sensors.put(id, sensor);
				return sensor;
			}
		}

		public Iterator<Entry<Integer, Sensor>> getSensorIterator() {
			return sensors.entrySet().iterator();
		}

		public boolean sensorExists(int id) {
			return sensors.containsKey(id);
		}

		public Sensor getSensor(int id) {
			return sensors.get(id);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + machine;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Machine other = (Machine) obj;
			if (machine != other.machine)
				return false;
			return true;
		}

		public String print() {
			StringBuilder builder = new StringBuilder();
			builder.append(this.toString() + "\n");
			for (Entry<Integer, Sensor> entry : sensors.entrySet()) {

				builder.append(String.format("%s\n", entry.getValue().toString()));
			}
			builder.setLength(builder.length() - 1);
			return builder.toString();
		}

		@Override
		public String toString() {
			return String.format("Machine [machine=%04d, type=%s, sensors=%s]", machine, type, sensors.size());
		}

	}

	public static class Sensor {
		public final int machine;
		public final int id;
		public final byte[] byteSensor;
		public final int k;
		public final double pt;

		public Sensor(int machine, int id, int k, double pt) {
			super();
			this.machine = machine;
			this.id = id;
			this.byteSensor = String.format("%d_%d", machine, id).getBytes();
			this.k = k;
			this.pt = pt;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			result = prime * result + machine;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Sensor other = (Sensor) obj;
			if (id != other.id)
				return false;
			if (machine != other.machine)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return String.format("Sensor [machine=%04d, id=%04d, k=%03d, pt=%1.5f]", machine, id, k, pt);
		}

	}

	public static void main(String[] args) {
		String path = new File(System.getProperty("user.home") + "/workdir/debs17gc/data/1000molding_machine.metadata.nt").getPath();
		MetaData.load(path);
		System.out.println(MetaData.print());
		System.out.println(MetaData.stats());
	}
}
