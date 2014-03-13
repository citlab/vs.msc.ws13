package de.tu_berlin.citlab.db;

import java.io.Serializable;

public class Counter implements Serializable
{
	private String countkey;
	private String countname;
	
	public Counter( String countkey, String countname )
	{
		this.countkey = countkey;
		this.countname = countname;
	}
	
	public String getCountKey()
	{
		return this.countkey;
	}
	
	public String getCountName()
	{
		return this.countname;
	}
}
