package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;

public interface IKeyConfig extends AbstractKeyConfig<List<Object>, Fields, Fields>
{
	public List<Object> sortWithKey(Fields input, Fields keyFields);
}

abstract interface AbstractKeyConfig<K, I, J> extends Serializable
{
	abstract public K sortWithKey(I input, J keyFields);
}