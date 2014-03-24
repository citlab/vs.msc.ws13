package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import backtype.storm.tuple.Tuple;

public class WindowHandler implements Window<Tuple, List<List<Tuple>>> {

    private static final long serialVersionUID = 1L;

	/* Global Constants: */
	/* ================= */

    final protected Map<Serializable, Window<Tuple, List<Tuple>>> windows;
    final Window<Tuple, List<Tuple>> stub;
    /**
     * By assigning window-keys to tuples, multiple windows can be created for
     * (semantically) different tuples.
     */
    final protected IKeyConfig windowKey;
    /**
     * By assigning group-keys to tuples, groups can be created WITHIN one
     * window. Once a window is satisfied its inner groups are extracted and
     * returned by the flush method, just as if each group would have been a
     * distinct window before
     */
    final protected IKeyConfig groupByKey;

    /**
     * reference to parent UDF bolt, important to enable logging
     */
    private UDFBolt bolt;

    public void setUDFBolt(UDFBolt bolt){
        this.bolt=bolt;
    }

    public UDFBolt getUDFBolt(){ return this.bolt; }

	/* Constructors: */
	/* ============= */

    public WindowHandler(Window<Tuple, List<Tuple>> stub) {
        this(stub, KeyConfigFactory.DefaultKey());
    }

    public WindowHandler(Window<Tuple, List<Tuple>> stub, IKeyConfig windowKey) {
        this(stub, windowKey, KeyConfigFactory.DefaultKey());
    }

    public WindowHandler(Window<Tuple, List<Tuple>> stub, IKeyConfig windowKey, IKeyConfig groupByKey) {
        this.stub = stub;
        this.windowKey = windowKey;
        this.groupByKey = groupByKey;

        windows = new HashMap<Serializable, Window<Tuple, List<Tuple>>>();
    }


    /* Public Methods: */
	/* =============== */

    public IKeyConfig getWindowKey() {
        return windowKey;
    }

    public IKeyConfig getGroupByKey() {
        return groupByKey;
    }


    /* Public Methods: */
	/* =============== */

    public void add(Tuple input) {
        Serializable key = windowKey.getKeyOf(input);
        if (!windows.containsKey(key)) {
            windows.put(key, stub.clone());
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
            if (window instanceof TimeWindow || window.isSatisfied()) {

                // provide statistics
                getUDFBolt().log_statistics( getUDFBolt().getUDFDescription() + " - process window [ key: "+key+", "+window.getClass().getSimpleName()+", size:"+window.size() );

                Map<Serializable, List<Tuple>> groups = new HashMap<Serializable, List<Tuple>>();
                for(Tuple tuple : window.flush()) {
                    Serializable groupKey = groupByKey.getKeyOf(tuple);
                    if(!groups.containsKey(groupKey)) {
                        groups.put(groupKey, new ArrayList<Tuple>());
                    }
                    groups.get(groupKey).add(tuple);
                }
                for(List<Tuple> group : groups.values()) {
                    result.add(group);
                }

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

    @Override
    public int size() {
        int size=0;
        for (Object key : windows.keySet()) {
            Window<Tuple, List<Tuple>> window = windows.get(key);
            size = size + window.size();
        }
        return size;
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