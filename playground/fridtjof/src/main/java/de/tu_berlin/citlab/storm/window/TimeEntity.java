package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimeEntity<I> implements Comparable<TimeEntity<I>> {
	
	final long timestamp;
	
	final I entity;
	
	public TimeEntity(I entity) {
		this(entity, System.currentTimeMillis());
	}
	
	public TimeEntity(I entity, long timestamp) {
		this.entity = entity;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public I getEntity() {
		return entity;
	}

	public int compareTo(TimeEntity<I> o) {
		return (int) (timestamp - o.getTimestamp());
	}
	
	public String toString() {
		return "value: " + entity + "; timestamp: " + timestamp;
	}
	
	public static void main(String[] args) {
		List<TimeEntity<Integer>> list = new ArrayList<TimeEntity<Integer>>();
		list.add(new TimeEntity<Integer>(1, 1L));
		list.add(new TimeEntity<Integer>(2, 2L));
		list.add(new TimeEntity<Integer>(10, 10L));
		list.add(new TimeEntity<Integer>(4, 4L));
		list.add(new TimeEntity<Integer>(6, 6L));
		list.add(new TimeEntity<Integer>(9, 9L));
		list.add(new TimeEntity<Integer>(7, 7L));
		list.add(new TimeEntity<Integer>(3, 3L));
		list.add(new TimeEntity<Integer>(8, 8L));
		list.add(new TimeEntity<Integer>(5, 5L));
		Collections.sort(list);
		System.out.println(list);
	}

}
