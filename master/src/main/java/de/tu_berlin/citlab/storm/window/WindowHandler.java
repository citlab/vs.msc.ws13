package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public class WindowHandler implements Window<Tuple, List<List<Tuple>>> {

	private static final long serialVersionUID = 1L;

/* Global Constants: */
/* ================= */
	
	final protected Map<Serializable, Window<Tuple, List<Tuple>>> windows;
	final Window<Tuple, List<Tuple>> stub;
	
	final protected IKeyConfig keyConfig;
	final protected TupleComparator sortByKey;
	
	private static int instanceIdCounter = 0;
	
	public int instanceId = instanceIdCounter++;
	
	

/* Constructors: */
/* ============= */
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub) {
		this(stub, null, null );
	}

	public WindowHandler(Window<Tuple, List<Tuple>> stub, IKeyConfig keyConfig) {
		this(stub, keyConfig, null );
	}
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub, IKeyConfig keyConfig, TupleComparator sortByKey) {
		this.stub = stub;
		this.keyConfig = keyConfig;
		this.sortByKey = sortByKey;
		
		windows = new HashMap<Serializable, Window<Tuple, List<Tuple>>>();
	}
	
	
/* Public Methods: */
/* =============== */

	public void add(Tuple input) {
		Serializable key = keyConfig.getKeyOf( input );
		if ( ! windows.containsKey(key)) {
			windows.put(key, stub.clone());
		} else {
		}
		Window<Tuple, List<Tuple>> window = windows.get(key);
		window.add(input);
	}

	public boolean isSatisfied() {
		boolean result = false;
		for (Object key : windows.keySet()) {
			Window<Tuple, List<Tuple>> window = windows.get(key);
			result |= window.isSatisfied();
		}
		return result;
	}

	public List<List<Tuple>> flush() {
		List<List<Tuple>> result = new ArrayList<List<Tuple>>();
		for (Object key : windows.keySet()) {
			Window<Tuple, List<Tuple>> window = windows.get(key);
			if (window.isSatisfied()) {
				List<Tuple> windowTuples = window.flush();

				if(sortByKey!=null){
					Collections.sort( windowTuples, sortByKey );
					result.add(windowTuples);
				} else {
					result.add(windowTuples);
				}

			}//if
		}
		return result;
	}

	
	public Window<Tuple, List<Tuple>> getStub() {
		return stub;
	}

	public WindowHandler clone() {
		return null;
	}

	public List<List<Tuple>> addSafely(Tuple input) {
		List<List<Tuple>> result = null;
		if (isSatisfied()) {
			result = flush();
		}
		add(input);
		return result;
	}

}
