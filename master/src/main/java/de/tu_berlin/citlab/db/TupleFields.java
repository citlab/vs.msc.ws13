package de.tu_berlin.citlab.db;

import java.io.Serializable;
import backtype.storm.tuple.Fields;

public class TupleFields implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	Fields fields;
	
	public TupleFields() {};
	
	public TupleFields(Fields fields)
	{
		this.fields = fields;
	}
	
	public Fields getTupleFields()
	{
		return fields;
	}

}
