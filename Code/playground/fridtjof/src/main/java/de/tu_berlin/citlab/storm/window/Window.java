package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Window<I> implements Serializable {

	private static final long serialVersionUID = 3210792646347151651L;

	/**
	 * data structure that holds entities
	 */
	List<I> slots;

	/**
	 * maximum amount of slots within this windows
	 */
	int size;

	/**
	 * amount of new free slots after flush
	 */
	int offset;

	public Window(int size) {
		this(size, size);
	}

	public Window(int size, int offset) {
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

	public boolean isFull() {
		return slots.size() == size;
	}

	/**
	 * returns the current load of entities and removes as much entities from
	 * the first slots as configured by the second parameter of
	 * {@link Window#Window(int, int)}
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

}
