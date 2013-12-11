package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public interface IKeyConfig extends AbstractKeyConfig<List<Object>, Tuple, Fields>
{
	public List<Object> sortWithKey(Tuple input, Fields keyFields);
}

abstract interface AbstractKeyConfig<K, I, J> extends Serializable
{
	abstract public K sortWithKey(I input, J keyFields);
}