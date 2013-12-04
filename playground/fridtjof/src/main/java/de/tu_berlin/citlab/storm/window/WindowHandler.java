package de.tu_berlin.citlab.storm.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WindowHandler implements Window<Tuple, List<List<Tuple>>> {

	private static final long serialVersionUID = -3653875602558667831L;

	protected Map<List<Object>, Window<Tuple, List<Tuple>>> windows;

	protected List<Object> defaultKey = new ArrayList<Object>();

	final Window<Tuple, List<Tuple>> stub;

	final protected Fields keyFields;

	public WindowHandler(Window<Tuple, List<Tuple>> stub) {
		this(stub, null);
	}

	public WindowHandler(Window<Tuple, List<Tuple>> stub, Fields keyFields) {
		this.stub = stub;
		this.keyFields = keyFields;
		windows = new HashMap<List<Object>, Window<Tuple, List<Tuple>>>();
	}

	public void add(Tuple input) {
		List<Object> key;
		if (keyFields == null) {
			key = defaultKey;
		} else {
			key = input.select(keyFields);
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
