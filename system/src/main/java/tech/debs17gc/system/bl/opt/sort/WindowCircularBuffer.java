package tech.debs17gc.system.bl.opt.sort;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;

public class WindowCircularBuffer<T extends Tuple> implements Iterable<T>, Iterator<T> {

	private final int finalSize;
	private final Tuple buffer[];

	private int pointer = 0;
	private int currentSize = 0;

	private int iteratorPointer = 0;

	private int count = -1;

	public WindowCircularBuffer(int size) {
		this.finalSize = size;
		this.buffer = new Tuple[size];
	}

	public void add(Tuple t) {
		if (currentSize == finalSize) {
			buffer[pointer] = t;
			pointer = (pointer + 1) % finalSize;
		} else {
			buffer[currentSize] = t;
			currentSize++;
		}
		count++;
	}

	public int count() {
		return count;
	}

	public int size() {
		return currentSize;
	}

	@SuppressWarnings("unchecked")
	public T getLatest() {
		return (T) buffer[(pointer + currentSize - 1) % finalSize];
	}

	@SuppressWarnings("unchecked")
	public T getLatest(int offset) {
		if (offset >= currentSize) {
			return null;
		}
		return (T) buffer[(pointer + currentSize - offset - 1) % finalSize];
	}

	@SuppressWarnings("unchecked")
	public T getOldest() {
		return (T) buffer[pointer];
	}

	@Override
	public Iterator<T> iterator() {
		iteratorPointer = 0;
		return this;
	}

	@Override
	public boolean hasNext() {
		return iteratorPointer < currentSize;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T next() {
		if (iteratorPointer >= finalSize)
			return null;
		iteratorPointer++;
		return (T) buffer[(pointer + iteratorPointer - 1) % finalSize];
	}

	@Override
	public String toString() {
		return "WindowCircularBuffer [size=" + finalSize + ", buffer=" + Arrays.toString(buffer) + ", initPointer=" + pointer + ", count="
				+ currentSize + ", iteratorPointer=" + iteratorPointer + "]";
	}

	public static void main(String[] args) {
		WindowCircularBuffer<Tuple1<Integer>> buffer = new WindowCircularBuffer<>(3);
		System.out.println(buffer);
		for (int j = 0; j < 20; j++) {
			buffer.add(new Tuple1<>(j));
			System.out.println(buffer);

			Iterator<Tuple1<Integer>> it = buffer.iterator();
			while (it.hasNext()) {
				Tuple1<Integer> next = it.next();
				System.out.println(next);
			}

		}

	}
}
