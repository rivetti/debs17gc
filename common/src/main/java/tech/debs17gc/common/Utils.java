package tech.debs17gc.common;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;

public class Utils {

	public static ToBePrintedVector toString(double[] a) {
		return new ToBePrintedVector(a);
	}

	public static ToBePrintedMatrix printMatrix(double[][] matrix) {
		return new ToBePrintedMatrix(matrix);
	}

	public static class ToBePrintedVector {
		private final double[] matrix;

		public ToBePrintedVector(double[] matrix) {
			super();
			this.matrix = matrix;
		}

		@Override
		public String toString() {
			return Arrays.toString(matrix);
		}

	}

	public static class ToBePrintedMatrix {
		private final double[][] matrix;

		public ToBePrintedMatrix(double[][] matrix) {
			super();
			this.matrix = matrix;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < matrix.length; i++) {
				builder.append(Arrays.toString(matrix[i]));
				builder.append("\n");
			}
			builder.setLength(builder.length() - 1);
			return builder.toString();
		}

	}

}
