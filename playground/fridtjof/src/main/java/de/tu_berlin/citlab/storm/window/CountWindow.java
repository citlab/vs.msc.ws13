package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.List;

public class CountWindow<I> implements Window<I, List<I>> {

	private static final long serialVersionUID = 3210792646347151651L;

	/**
	 * maximum amount of slots within this windows
	 */
	final int size;

	/**
	 * amount of new free slots after flush
	 */
	final int offset;

	/**
	 * data structure that holds entities
	 */
	List<I> slots;

	public CountWindow(int size) {
		this(size, size);
	}

	public CountWindow(int size, int offset) {
		if (offset > size) {
			throw new IllegalArgumentException(
					"offset must not be larger than size");
		}
		this.size = size;
		this.offset = offset;
		slots = new ArrayList<I>();
	}

	public void add(I entity) {
		if (slots.size() >= size) {
			throw new ArrayIndexOutOfBoundsException();
		}
		slots.add(entity);
	}

	public boolean isSatisfied() {
		return slots.size() >= size;
	}

	/**
	 * returns the current load of entities and removes as much entities from
	 * the first slots as configured by the second parameter of
	 * {@link CountWindow#Window(int, int)}
	 * 
	 * @return
	 */
	public List<I> flush() {
		List<I> result = new ArrayList<I>(slots);
		for (int i = 0; i < offset; i++) {
			if (slots.size() == 0) {
				break;
			}
			slots.remove(0);
		}
		return result;
	}

	public CountWindow<I> clone() {
		return new CountWindow<I>(size, offset);
	}
	
	@Override
	public String toString() {
		return slots.toString();
	}

}
