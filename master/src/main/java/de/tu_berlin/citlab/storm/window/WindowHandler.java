package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.udf.IKeyConfig;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WindowHandler implements Window<Tuple, List<List<Tuple>>> {

	private static final long serialVersionUID = 1L;

/* Global Constants: */
/* ================= */
	
	final protected Map<List<Object>, Window<Tuple, List<Tuple>>> windows;
	final Window<Tuple, List<Tuple>> stub;
	
	final protected List<Object> defaultKey = new ArrayList<Object>();
	final protected Fields keyFields;
	final protected IKeyConfig keyConfig;
	
	

/* Constructors: */
/* ============= */
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub) {
		this(stub, null, null);
	}

	public WindowHandler(Window<Tuple, List<Tuple>> stub, Fields keyFields) {
		this(stub, keyFields, null);
	}
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub, Fields keyFields, IKeyConfig keyConfig) {
		this.stub = stub;
		this.keyFields = keyFields;
		this.keyConfig = keyConfig;
		
		windows = new HashMap<List<Object>, Window<Tuple, List<Tuple>>>();
	}
	
	
	
/* Public Methods: */
/* =============== */

	public void add(Tuple input) {
		List<Object> key;
		if (keyFields == null) {
			key = defaultKey;
		} else {
			if(keyConfig == null)
				key = input.select(keyFields);
			else
				key = keyConfig.sortWithKey( input, keyFields);
		}
		if ( ! windows.containsKey(key)) {
			windows.put(key, stub.clone());
		}
		Window<Tuple, List<Tuple>> window = windows.get(key);
		window.add(input);
	}

	public boolean isSatisfied() {
		boolean result = false;
		for (List<Object> key : windows.keySet()) {
			Window<Tuple, List<Tuple>> window = windows.get(key);
			result |= window.isSatisfied();
		}
		return result;
	}

	public List<List<Tuple>> flush() {
		List<List<Tuple>> result = new ArrayList<List<Tuple>>();
		for (List<Object> key : windows.keySet()) {
			Window<Tuple, List<Tuple>> window = windows.get(key);
			
			if (window.isSatisfied()) {
				result.add(window.flush());
				
			}
		}
		return result;
		
	}

	
	public Window<Tuple, List<Tuple>> getStub() {
		return stub;
	}

	public WindowHandler clone() {
		return null;
	}

}
