package tech.debs17gc.system.bl.opt.markov;

import org.apache.flink.api.java.tuple.Tuple5;

public class Cluster {
	public final int id;
	public double center;

	private double sum = 0.0;
	double count = 0.0;

	public Cluster(int id) {
		super();
		this.id = id;
		this.center = Double.NaN;
	}

	public void updateCluster(Tuple5<Integer, Integer, Long, Integer, Double> tuple) {
		this.sum += tuple.f4;
		this.count++;
	}

	public void setNewCenter(double center) {
		this.center = center;
		this.sum = 0;
		this.count = 0;
	}

	public double computeNewCenter() {
		return sum / count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(center);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + id;
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
		Cluster other = (Cluster) obj;
		if (Double.doubleToLongBits(center) != Double.doubleToLongBits(other.center))
			return false;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Cluster [id=" + id + ", center=" + center + ", sum=" + sum + ", count=" + count + "]";
	}

	public static Cluster search(Cluster[] clusters, int clusterNum, double key) throws RuntimeException {

		int low = 0;
		int high = clusterNum - 1;

		while (high >= low) {
			int middle = (low + high) / 2;
			if (clusters[middle].center == key) {
				return clusters[middle];
			}

			if (clusters[middle].center < key) {
				low = middle + 1;
				if (low > clusterNum - 1) {
					return clusters[middle];
				} else if (clusters[low].center > key) {
					double ceilingDistance = Math.abs(clusters[low].center - key);
					double floorDistance = Math.abs(clusters[middle].center - key);
					if (ceilingDistance <= floorDistance) {
						return clusters[low];
					} else {
						return clusters[middle];
					}
				}
			}
			if (clusters[middle].center > key) {
				high = middle - 1;
				if (high < 0) {
					return clusters[middle];
				} else if (clusters[high].center < key) {
					double ceilingDistance = Math.abs(clusters[middle].center - key);
					double floorDistance = Math.abs(clusters[high].center - key);
					if (ceilingDistance <= floorDistance) {
						return clusters[middle];
					} else {
						return clusters[high];
					}
				}
			}
		}

		throw new RuntimeException();

	}

	public static Cluster getCloserCentroid(Cluster[] clusters, int clusterNum, Tuple5<Integer, Integer, Long, Integer, Double> tuple) {
		return search(clusters, clusterNum, tuple.f4);
	}

}