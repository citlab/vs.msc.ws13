package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;
import backtype.storm.tuple.Tuple;

public interface IKeyConfig extends Serializable
{
	public Serializable getKeyOf(Tuple input);
}