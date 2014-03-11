package de.tu_berlin.citlab.db;

import java.io.Serializable;

import backtype.storm.tuple.Fields;

public class CassandraConfig implements DBConfig, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private String IP;
	private PrimaryKey primaryKeys;
	private Fields tupleFields;
	private String keyspace;
	private String table;
	
	public void setParams( String keyspace, String table, PrimaryKey primaryKeys, Fields tupleFields )
	{
		this.primaryKeys = primaryKeys;
		this.tupleFields = tupleFields;
		this.keyspace = keyspace;
		this.table = table;
	}
	
	/**
	 * @return keyspace
	 */
	public String getKeyspace()
	{
		return keyspace;
	}

	/**
	 * @return table
	 */
	public String getTable()
	{
		return table;
	}

	public CassandraConfig()
	{
		
	}
	
	/**
	 * @return iP
	 */
	public String getIP()
	{
		return IP;
	}
	
	public void setIP( String IP )
	{
		this.IP = IP;
	}

	/**
	 * @return primaryKeys
	 */
	public PrimaryKey getPrimaryKeys()
	{
		return primaryKeys;
	}

	/**
	 * @return tupleFields
	 */
	public Fields getTupleFields()
	{
		return tupleFields;
	}
	
/*	*//**
	 * @return keyspace
	 *//*
	public String getKeyspace()
	{
		return keyspace;
	}*/


}
