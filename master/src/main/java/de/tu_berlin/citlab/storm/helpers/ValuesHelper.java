package de.tu_berlin.citlab.storm.helpers;

import java.util.List;
import backtype.storm.tuple.Values;

public class ValuesHelper {
	public static Values fromObjectList(List<Object> tuple){
		return new Values(tuple);
	}
}
