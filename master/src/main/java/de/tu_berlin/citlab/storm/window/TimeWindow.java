package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class TimeWindow<I> implements Window<I, List<I>> {

	private static final long serialVersionUID = 3210792646347151651L;
	private static final Logger log = Logger.getLogger(TimeWindow.class);

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
			System.out.println("offset (" + offset
					+ ") must not be larger than size(" + timeSlot + ")");
			throw new IllegalArgumentException("offset (" + offset
					+ ") must not be larger than size(" + timeSlot + ")");

		}
		this.timeSlot = timeSlot;
		this.offset = offset;
		slots = new ArrayList<TimeEntity<I>>();
	}

	private long getAquiredTimeSlot() {
		long result = 0;
		if (!slots.isEmpty()) {
			long youngestAquiredTime = slots.get(0).getTimestamp();
			long oldestAquiredTime = TimeEntity.getcurrentTime();
			result = oldestAquiredTime - youngestAquiredTime;
		}
		return result;
	}

	public void add(I entity) {
		slots.add(new TimeEntity<I>(entity));
		Collections.sort(slots);
	}

	/**
	 * this window does not decide itself when it is satisfied. It is depended
	 * on external timing
	 */
    public boolean isSatisfied() {
        return false;
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
		for (TimeEntity<I> slot : slots) {
			result.add(slot.getEntity());
		}
		while (!slots.isEmpty() && getAquiredTimeSlot() + offset >= timeSlot) {
			slots.remove(0);
		}
		return result;
	}

	public TimeWindow<I> clone() {
		TimeWindow<I> newInstance = new TimeWindow<I>(timeSlot, offset);
		return newInstance;
	}

	@Override
	public String toString() {
		return slots.toString();
	}

	public long getTimeSlot() {
		return timeSlot;
	}

	public List<I> addSafely(I entity) {
		add(entity);
		return null;
	}
}
