package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimeWindow<I> implements Window<I, List<I>> {

	private static final long serialVersionUID = 3210792646347151651L;

	/**
	 * maximum amount of slots within this windows
	 */
	final int timeSlot;

	/**
	 * amount of new free slots after flush
	 */
	final int offset;

	/**
	 * data structure that holds entities
	 */
	List<TimeEntity<I>> slots;

	public TimeWindow(int timeSlot) {
		this(timeSlot, timeSlot);
	}

	public TimeWindow(int timeSlot, int offset) {
		if (offset > timeSlot) {
			throw new IllegalArgumentException(
					"offset must not be larger than size");
		}
		this.timeSlot = timeSlot;
		this.offset = offset;
		slots = new ArrayList<TimeEntity<I>>();
	}

	public void add(I entity) {
		if (isSatisfied()) {
			throw new ArrayIndexOutOfBoundsException();
		}
		slots.add(new TimeEntity<I>(entity));
		Collections.sort(slots);
	}

	public boolean isSatisfied() {
		boolean result = false;
		if (!slots.isEmpty()) {
			long aquiredTimeSlot = slots.get(slots.size() - 1).getTimestamp()
					- slots.get(0).getTimestamp();
			result = aquiredTimeSlot >= (long) (timeSlot * 1000);
		}
		return result;
	}

	/**
	 * returns the current load of entities and removes as much entities from
	 * the first slots as configured by the second parameter of
	 * {@link TimeWindow#Window(int, int)}
	 * 
	 * @return
	 */
	public List<I> flush() {
		List<I> result = new ArrayList<I>();
		for(TimeEntity<I> slot : slots) {
			result.add(slot.getEntity());
		}
		long youngestAcceptableTimestamp = slots.get(0).getTimestamp() + (long) (offset * 1000);
		boolean clean = false;
		while (!clean) {
			if(slots.get(0).getTimestamp() < youngestAcceptableTimestamp) {
				slots.remove(0);
			}
			else {
				clean = true;
			}
		}
		return result;
	}

	public TimeWindow<I> clone() {
		return new TimeWindow<I>(timeSlot, offset);
	}

	@Override
	public String toString() {
		return slots.toString();
	}

	public long getTimeSlot() {
		return timeSlot;
	}

}
