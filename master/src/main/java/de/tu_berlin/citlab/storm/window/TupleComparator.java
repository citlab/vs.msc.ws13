package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.Comparator;

import backtype.storm.tuple.Tuple;


@SuppressWarnings("serial")
abstract public class TupleComparator implements Serializable, Comparator<Tuple> { 

	@Override
	abstract public int compare(Tuple o1, Tuple o2);	
	abstract public Serializable getTupleKey(Tuple t);
}