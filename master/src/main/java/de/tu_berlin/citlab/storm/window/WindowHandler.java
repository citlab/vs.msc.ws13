package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.ArrayList;
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
	
	
	private static int instanceIdCounter = 0;
	
	public int instanceId = instanceIdCounter++;
	
	

/* Constructors: */
/* ============= */
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub) {
		this(stub, null);
	}
	
	public WindowHandler(Window<Tuple, List<Tuple>> stub, IKeyConfig keyConfig) {
		this.stub = stub;
		this.keyConfig = keyConfig;
		
		windows = new HashMap<Serializable, Window<Tuple, List<Tuple>>>();
	}
	
	
	
/* Public Methods: */
/* =============== */

	public void add(Tuple input) {
		Serializable key = keyConfig.getKeyOf( input );
		if ( ! windows.containsKey(key)) {
			windows.put(key, stub.clone());
//			if(windows.get(key) instanceof TimeWindow)
//			System.out.println("WindowHandler '" + instanceId + "' instanciated Window '" + ((TimeWindow) windows.get(key)).instanceId + "' for key '" + key.toString() + "'");
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

	public List<List<Tuple>> addSafely(Tuple input) {
		List<List<Tuple>> result = null;
		if (isSatisfied()) {
			//System.out.println("WindowHandler '" + instanceId + "' is satisfied");
			result = flush();
		}
		add(input);
		return result;
	}

}
