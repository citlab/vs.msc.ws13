package de.tu_berlin.citlab.db;

import java.io.Serializable;

public class PrimaryKey implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	String[] fields;
	
	public PrimaryKey() {};
	
	public PrimaryKey( String...strings )
	{
		this.fields = strings;
	}
	
	public String[] getPrimaryKeyFields()
	{
		return fields;
	}
	
}
