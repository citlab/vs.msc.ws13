package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public interface IKeyConfig extends Serializable
{
	public Serializable getKeyOf(Tuple input);
}