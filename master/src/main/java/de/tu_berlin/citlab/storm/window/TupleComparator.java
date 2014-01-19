package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.Comparator;

import backtype.storm.tuple.Tuple;


@SuppressWarnings("serial")
public class TupleComparator implements Serializable, Comparator<Tuple> { 

	@Override
	public int compare(Tuple o1, Tuple o2) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}