package de.tu_berlin.citlab.storm.helpers;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;

public class ValuesHelper {
	public static Values fromObjectList(List<Object> tuple){
		return new Values(tuple);
	}
	public static List<Object> toObjectList(Values val){
		List<Object> out = new ArrayList<Object>();
		for(Object v : val) out.add(v);
		return out;
	}
}
