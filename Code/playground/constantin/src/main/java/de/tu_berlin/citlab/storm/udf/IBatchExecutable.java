package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

public interface IBatchExecutable<K, I, O> extends Serializable {
	
	public O execute_batch(HashMap<K, Iterator<I>> entryMap); 
	public K sortBy_winKey(I param);
}
