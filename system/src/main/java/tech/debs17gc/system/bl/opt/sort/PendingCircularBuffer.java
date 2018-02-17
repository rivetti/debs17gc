package tech.debs17gc.system.bl.opt.sort;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PendingCircularBuffer<T extends Tuple> implements Iterable<T>, Iterator<T> {

	private final Logger LOGGER = LoggerFactory.getLogger(PendingCircularBuffer.class);

	private Tuple buffer[];
	private int targetSize;

	private final long tsStep;
	private long smallestTS;
	private int smallestTSPointer = -1;
	private long greaterTS;
	private int greaterTSPointer;

	private int iteratorPointer = 0;

	private int currentSize = 0;

	public PendingCircularBuffer(int size, long tsStep) {
		this.tsStep = tsStep;
		this.targetSize = size;
		this.buffer = new Tuple[size];
	}

	public void setInitialTS(long ts) {
		smallestTS = ts;
		smallestTSPointer = 0;
		greaterTS = ts;
		greaterTSPointer = 0;
	}

	public void put(long ts, Tuple t) {
		if (smallestTSPointer == -1) {
			setInitialTS(ts);
		}
		update(ts, t);
		currentSize++;
	}

	private void update(long ts, Tuple t) {
		if (ts == smallestTS) {
			buffer[smallestTSPointer] = t;
			LOGGER.debug("Smallest TS hit, added {} at {} ({})", t, smallestTSPointer, smallestTS);
			return;
		}

		final int offsetPos = getOffset(ts);
		if (Math.abs(offsetPos) >= targetSize) {
			increase(offsetPos);
			update(ts, t);
			return;

		}

		final int pos = getPos(offsetPos);
		if (smallestTS < ts) {
			buffer[pos] = t;
			if (ts > greaterTS) {
				greaterTS = ts;
				greaterTSPointer = pos;
			}
			LOGGER.debug("Larger, added {} at {} ({})", t, pos, ts);
			// Only in transient phase
		} else if (smallestTS > ts) {
			if ((greaterTSPointer < smallestTSPointer && greaterTSPointer < pos && pos < smallestTSPointer)
					|| (smallestTSPointer <= greaterTSPointer && (greaterTSPointer < pos || pos < smallestTSPointer))) {
				buffer[pos] = t;
				smallestTS = ts;
				smallestTSPointer = pos;
				LOGGER.debug("Smaller, added {} at {} ({})", t, pos, ts);
			} else {
				increase(offsetPos);
				update(ts, t);
				return;
			}
		}

	}

	public boolean hasSmallest() {
		return buffer[smallestTSPointer] != null;
	}

	@SuppressWarnings("unchecked")
	public T getSmallest() {
		LOGGER.debug("Return smallest {} at {}, ({})", buffer[smallestTSPointer], smallestTSPointer, smallestTS);
		return (T) buffer[smallestTSPointer];
	}

	@SuppressWarnings("unchecked")
	public T removeSmallest() {
		LOGGER.debug("Remove smallest {} at {}, ({})", buffer[smallestTSPointer], smallestTSPointer, smallestTS);
		Tuple t = buffer[smallestTSPointer];
		if (t != null) {
			buffer[smallestTSPointer] = null;
			smallestTSPointer = (smallestTSPointer + 1) % targetSize;
			smallestTS += tsStep;
			currentSize--;
			LOGGER.debug("Update {}, {}, {}", smallestTSPointer, smallestTS, currentSize);
		}
		return (T) t;
	}

	@SuppressWarnings("unchecked")
	public T get(long ts) {
		int pos = getPos(getOffset(ts));
		LOGGER.debug("Return {} at {}, ({})", buffer[pos], ts, pos);
		return (T) buffer[pos];
	}

	@SuppressWarnings("unchecked")
	public T remove(long ts) {
		int pos = getPos(getOffset(ts));
		LOGGER.debug("Remove {} at {}, ({})", buffer[pos], ts, pos);
		Tuple t = buffer[pos];
		if (t != null) {
			buffer[pos] = null;
			smallestTSPointer = (pos + 1) % targetSize;
			smallestTS = ts + tsStep;
			currentSize--;
			LOGGER.debug("Update {}, {}, {}", smallestTSPointer, smallestTS, currentSize);
		}
		return (T) t;
	}

	public int size() {
		return currentSize;
	}

	public boolean isEmpty() {
		return this.size() == 0;
	}

	private int getOffset(long ts) {
		long offSetTS = ts - smallestTS;
		return (int) (offSetTS / tsStep);
	}

	private int getPos(int offsetPos) {
		int pos = (smallestTSPointer + offsetPos) % targetSize;
		if (pos >= 0) {
			return pos;
		} else {
			return targetSize + pos;
		}
	}

	@Override
	public Iterator<T> iterator() {
		iteratorPointer = 0;
		return this;
	}

	@Override
	public boolean hasNext() {
		return iteratorPointer < targetSize;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T next() {
		if (iteratorPointer >= targetSize)
			return null;
		iteratorPointer++;
		return (T) buffer[(smallestTSPointer + iteratorPointer - 1) % targetSize];
	}

	@Override
	public String toString() {
		return "PendingCircularBuffer [buffer=" + Arrays.toString(buffer) + ", targetSize=" + targetSize + ", tsStep=" + tsStep
				+ ", smallestTS=" + smallestTS + ", smallestTSPointer=" + smallestTSPointer + ", greaterTS=" + greaterTS
				+ ", greaterTSPointer=" + greaterTSPointer + ", iteratorPointer=" + iteratorPointer + ", currentSize=" + currentSize + "]";
	}

	public void increase(int offsetPosition) {

		int newSize = Math.max(targetSize * 2, offsetPosition * 2);

		LOGGER.info("Increase from {} to {}", targetSize, newSize);

		Tuple[] aux = new Tuple[newSize];
		int largestNonNull = 0;
		for (int i = 0; i < targetSize; i++) {
			int pos = (smallestTSPointer + i) % targetSize;
			aux[i] = buffer[pos];
			if (aux[i] != null) {
				largestNonNull = 1;
			}
		}

		// Update
		smallestTSPointer = 0;
		greaterTSPointer = largestNonNull;
		greaterTS = smallestTS + largestNonNull * tsStep;
		buffer = aux;
		targetSize = newSize;
	}

	public static void main(String[] args) {
		PendingCircularBuffer<Tuple1<Integer>> buf = new PendingCircularBuffer<>(1, 10);
		buf.setInitialTS(20);
		System.out.println(buf);
		buf.put(3 * 10, new Tuple1<Integer>(3));
		System.out.println(buf);
		buf.put(4 * 10, new Tuple1<Integer>(4));
		System.out.println(buf);
		while (buf.hasSmallest()) {
			System.out.println(buf.removeSmallest());
		}
		System.out.println(buf);
		buf.put(0 * 10, new Tuple1<Integer>(0));

		System.out.println(buf);
		buf.put(1 * 10, new Tuple1<Integer>(1));
		System.out.println(buf);
		while (buf.hasSmallest()) {
			System.out.println(buf.removeSmallest());
		}
		System.out.println(buf);
		buf.put(2 * 10, new Tuple1<Integer>(2));
		System.out.println(buf);
		buf.put(5 * 10, new Tuple1<Integer>(5));
		System.out.println(buf);

		buf.put(20 * 10, new Tuple1<Integer>(20));
		System.out.println(buf);

		while (buf.hasSmallest()) {
			System.out.println(buf.removeSmallest());
		}

	}

}
