package tech.debs17gc.system.bl.opt.markov;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.debs17gc.common.metadata.MetaData;
import tech.debs17gc.system.bl.opt.sort.WindowCircularBuffer;

public class MarkovKMean {

	private static final Logger LOGGER = LoggerFactory.getLogger(MarkovKMean.class);

	private final int maxIterations;
	private final int windowSize;
	private final double convergenceThreshold;
	private final int transitionNum;
	private final int[] clusteringOutput;
	private Cluster[] clusters;
	private Cluster[] newClusters;
	private double[][] transitionMatrix;
	private double[] normalizingVector;
	/*
	 * DEBUG
	 */

	// TODO given the hashfunction, save locally cluster num info
	public MarkovKMean(int maxIterations, int windowSize, double convergenceThreshold, int transitionNum) {
		super();
		this.maxIterations = maxIterations;
		this.windowSize = windowSize;
		this.convergenceThreshold = convergenceThreshold;
		this.transitionNum = transitionNum;
		this.clusteringOutput = new int[windowSize];
		this.clusters = new Cluster[100];
		this.newClusters = new Cluster[100];
		for (int i = 0; i < clusters.length; i++) {
			this.clusters[i] = new Cluster(i);
			this.newClusters[i] = new Cluster(i);
		}
		this.transitionMatrix = new double[100][100];
		this.normalizingVector = new double[100];
	}

	public Tuple5<Integer, Integer, Integer, Integer, Double> apply(int machine, int sensor, int expectedClusters,
			WindowCircularBuffer<Tuple5<Integer, Integer, Long, Integer, Double>> window) {
		Tuple5<Integer, Integer, Long, Integer, Double> oldestTuple = window.getOldest();
		// Tuple5<Integer, Integer, Long, Integer, Double> oldestTuple =
		int timestamp = oldestTuple.f3;
		int clusteringOutputSize = 0;
		int clusterNum = 0;
		{

			/*
			 * Kmeans
			 */

			{
				final int targetClusterNum = MetaData.getMachine(machine).getSensor(sensor).k;
				TreeSet<Double> orderedCentroids = new TreeSet<>();
				HashSet<Double> centroids = new HashSet<>(targetClusterNum);
				// Initialize centroids
				LOGGER.debug("[{},{}][{}] ########## initialize centroids ({}) ##########", machine, sensor, timestamp, targetClusterNum);
				Iterator<Tuple5<Integer, Integer, Long, Integer, Double>> it = window.iterator();
				while (centroids.size() < targetClusterNum && it.hasNext()) {
					Tuple5<Integer, Integer, Long, Integer, Double> tuple = it.next();

					if (!centroids.contains(tuple.f4)) {
						orderedCentroids.add(tuple.f4);
						centroids.add(tuple.f4);
						LOGGER.debug("[{},{}][{}] new centroid: {}", machine, sensor, timestamp, tuple.f4);
					} else {
						// TODO happens too many times to log
						// LOGGER.warn("[{},{}][{}] Lost centroid: tuple {} matches centroid {}" machine, sensor, timestamp,
						// tuple, newCentroids.get(tuple.f4));
					}
				}

				int id = 0;
				for (Double centroid : orderedCentroids) {
					clusters[id++].setNewCenter(centroid);
				}
				// TODO reset clusters!
				clusterNum = id;
				orderedCentroids.clear();
				centroids.clear();

				if (false && clusterNum != targetClusterNum) {
					LOGGER.warn("[{},{}][{}] does not have exactly {} clusters:{}", machine, sensor, timestamp, targetClusterNum,
							clusterNum);
				}
			}
			double distance = Double.MAX_VALUE;
			// Iterate
			// TODO distance <= threshold by organizers reply
			for (int i = 0; i < maxIterations && distance > convergenceThreshold; i++) {
				LOGGER.debug("[{},{}][{}] iterations {} (previous distance: {})", machine, sensor, timestamp, i, distance);
				// Initialize iteration ds
				distance = 0;

				// Assign points to clusters
				Iterator<Tuple5<Integer, Integer, Long, Integer, Double>> it = window.iterator();
				int j = 0;
				for (; it.hasNext(); j++) {

					Tuple5<Integer, Integer, Long, Integer, Double> tuple = it.next();

					Cluster cluster = Cluster.getCloserCentroid(clusters, clusterNum, tuple);
					LOGGER.debug("[{},{}][{}] Assign tuple {} to cluster {}", machine, sensor, timestamp, tuple, cluster);
					cluster.updateCluster(tuple);

					clusteringOutput[j] = cluster.id;

				}

				clusteringOutputSize = j;

				// Recompute clusters
				LOGGER.debug("[{},{}][{}] Recompute centroids", machine, sensor, timestamp);

				for (int k = 0; k < clusterNum; k++) {

					if (clusters[k].count == 0) {
						LOGGER.debug("[{},{}][{}] Empty cluster", machine, sensor, timestamp, clusters[k]);
						LOGGER.debug("[{},{}][{}] Terminating Clustering", machine, sensor, timestamp, clusters[k]);
						distance = 0.0;
						break;

					}
					double newCenter = clusters[k].computeNewCenter();
					newClusters[k].setNewCenter(newCenter);
					distance += Math.abs(newCenter - clusters[k].center);
					LOGGER.debug("[{},{}][{}] new cluster: {}", machine, sensor, timestamp, newClusters[k]);

				}
				if (distance != 0.0) {
					Cluster[] aux = clusters;
					clusters = newClusters;
					newClusters = aux;
				}
			}

			LOGGER.debug("[{},{}][{}] ##### Clustering DONE with distance: {}", machine, sensor, timestamp, distance);
		}

		ArrayList<Tuple5<Integer, Integer, Long, Integer, Double>> inputList = null;
		if (LOGGER.isDebugEnabled()) {
			inputList = new ArrayList<>(windowSize);
			{

				Iterator<Tuple5<Integer, Integer, Long, Integer, Double>> it = window.iterator();

				while (it.hasNext()) {
					Tuple5<Integer, Integer, Long, Integer, Double> tuple = it.next();
					inputList.add(tuple);
				}

				LOGGER.debug("[{},{}][{}] Check TSs {}", machine, sensor, timestamp, inputList.size());
				for (int i = 1; i < inputList.size(); i++) {
					if (inputList.get(i - 1).f3 + 1 != inputList.get(i).f3) {
						LOGGER.error("[{},{}][{}] Inconsistent TSs : {} -> {}", machine, sensor, timestamp, inputList.get(i - 1),
								inputList.get(i));
					}
				}

			}
		}

		for (int i = 0; i < clusterNum; i++) {
			for (int j = 0; j < clusterNum; j++) {
				transitionMatrix[i][j] = 0.0;
			}
			normalizingVector[i] = 0.0;
		}
		{

			/*
			 * Train Markov Model
			 */
			for (int i = 1; i < clusteringOutputSize; i++) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("[{},{}][{}] Transition [{}]: {} -> {}, {} -> {}", machine, sensor, timestamp, i, clusteringOutput[i - 1],
							clusteringOutput[i], inputList.get(i - 1), inputList.get(i));
				}

				transitionMatrix[clusteringOutput[i - 1]][clusteringOutput[i]]++;
				normalizingVector[clusteringOutput[i - 1]]++;
			}

			/*
			 * if (LOGGER.isDebugEnabled()) { LOGGER.debug("[{},{}][{}] normalization vector: {}", machine, sensor, timestamp,
			 * Utils.toString(normalizingVector)); }
			 * 
			 * normalizeTransitionMatrix(transitionMatrix, normalizingVector, timestamp, machine, sensor);
			 * 
			 * if (LOGGER.isDebugEnabled()) { LOGGER.debug("[{},{}][{}] normalized transition matrix: \n{}", machine, sensor, timestamp,
			 * Utils.printMatrix(transitionMatrix));
			 * 
			 * }
			 */

			LOGGER.debug("[{},{}][{}] ##### Training DONE", machine, sensor, timestamp);
		}

		{

			/*
			 * Identify Anomaly
			 */
			// TODO check indexes
			LOGGER.debug("[{},{}][{}] Replay last {} transitions", machine, sensor, timestamp, transitionNum);
			double probability = 1.0;
			for (int i = clusteringOutputSize - transitionNum; i > 0 && i < clusteringOutputSize; i++) {
				probability *= transitionMatrix[clusteringOutput[i - 1]][clusteringOutput[i]] / normalizingVector[clusteringOutput[i - 1]];

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("[{},{}][{}] Transition [{}]: {} -> {} => {}/{} = {} ({}), {} -> {}", machine, sensor, timestamp, i,
							clusteringOutput[i - 1], clusteringOutput[i], transitionMatrix[clusteringOutput[i - 1]][clusteringOutput[i]],
							normalizingVector[clusteringOutput[i - 1]],
							transitionMatrix[clusteringOutput[i - 1]][clusteringOutput[i]] / normalizingVector[clusteringOutput[i - 1]],
							probability, inputList.get(i - 1), inputList.get(i));
				}
			}
			// Tuple5<Integer, Integer, Long, Integer, Double> triggeringTuple = window.getLatest(transitionNum);
			Tuple5<Integer, Integer, Integer, Integer, Double> output = new Tuple5<>(oldestTuple.f0, oldestTuple.f1, oldestTuple.f3,
					window.count(), probability);

			LOGGER.debug("[{},{}][{}] output: {}", machine, sensor, timestamp, output);

			return output;

		}

	}

	@Override
	public String toString() {
		return "MarkovKMean [maxIterations=" + maxIterations + ", countSize=" + windowSize + ", convergenceThreshold="
				+ convergenceThreshold + ", transitionNum=" + transitionNum + "]";
	}

}
