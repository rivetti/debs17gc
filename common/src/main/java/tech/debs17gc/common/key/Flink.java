package tech.debs17gc.common.key;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;

public class Flink {
	/**
	 * Computes the index of the operator to which a key-group belongs under the given parallelism and maximum parallelism.
	 *
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want to go beyond this
	 * boundary, this method must perform arithmetic on long values.
	 *
	 * @param maxParallelism
	 *            Maximal parallelism that the job was initially created with. 0 < parallelism <= maxParallelism <= Short.MAX_VALUE must
	 *            hold.
	 * @param parallelism
	 *            The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param keyGroupId
	 *            Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return The index of the operator to which elements from the given key-group should be routed under the given parallelism and
	 *         maxParallelism.
	 */
	public static int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
		return keyGroupId * parallelism / maxParallelism;
	}

	public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
		return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
	}

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param key
	 *            the key to assign
	 * @param maxParallelism
	 *            the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int assignToKeyGroup(Object key, int maxParallelism) {
		return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
	}

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param keyHash
	 *            the hash of the key to assign
	 * @param maxParallelism
	 *            the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
		return MathUtils.murmurHash(keyHash) % maxParallelism;
	}

	public Tuple getKey(Tuple value) {
		Tuple2<Integer, Integer> key = new Tuple2<>();
		for (int i = 0; i < 2; i++) {
			key.setField(value.getField(i), i);
		}
		return key;
	}

}
