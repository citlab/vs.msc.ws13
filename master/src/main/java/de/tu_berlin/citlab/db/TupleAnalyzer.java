package de.tu_berlin.citlab.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TupleAnalyzer
{
	String keyspace;
	String table;
	Fields fieldsInTuple;
	List <Object> objectsInTuple;
	List <String> cassandraTypesInTuple = new ArrayList <String>();
	List <String> javaTypesInTuple = new ArrayList <String>();
	String[] primaryKeys; // = CassandraBolt.config.getPrimaryKeys().getPrimaryKeyFields();

	Map <String, String> javaToCassandraTypes = new HashMap <String, String>()
	{
		private static final long serialVersionUID = 1L;

		{
			put( "String", "text" );
			put( "Integer", "int" );
			put( "Long", "bigint" );
		}
	};

	// A tuple analyzer retrieves fields and values from a tuple and maps them to Cassandra data types
	public TupleAnalyzer( Tuple tuple )
	{
		fieldsInTuple = tuple.getFields();
		objectsInTuple = tuple.getValues();
		System.out.println(objectsInTuple);
		getJavaAndCassandraTypesFromTupleObjects( objectsInTuple );
	}
	
	public void setPrimaryKey( PrimaryKey pk )
	{
		this.primaryKeys = pk.getPrimaryKeyFields();
	}

	public void getJavaAndCassandraTypesFromTupleObjects( List <Object> objects )
	{
		for ( Object obj : objects )
		{
			String canonical = obj.getClass().getCanonicalName();
			String[] parts = canonical.split( "\\." );
			String classname = parts[ parts.length - 1 ];
			javaTypesInTuple.add( classname );
			if ( javaToCassandraTypes.containsKey( classname ) )
			{
				cassandraTypesInTuple.add( javaToCassandraTypes.get( classname ) );
			}
			else
			{
				cassandraTypesInTuple.add( "blob" );
			}
			
		}
	}

	public String createKeyspaceQuery( String keyspace )
	{
		return String
				.format(
						"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
						keyspace );
	}
	
	public String createPreparedInsertStatement( String keyspace, String table )
	{
		List <String> fieldlist = fieldsInTuple.toList();
		String[] fields = fieldlist.toArray(new String[fieldlist.size()]);

		return "INSERT INTO " + keyspace + "." + table + " (" + 
				StringUtils.join( fields, "," ) + ") VALUES(" + 
				StringUtils.repeat( "? ", fields.length ).trim().replaceAll( " ", "," ) + ")";
	}

	public String createTableQueryByFields( String keyspace, String table, List <String> fields )
	{
		List <String> fieldsWithDataTypes = new ArrayList <String>();
		for ( int i = 0; i < fields.size(); i++ )
		{
			String str = fields.get( i ) + " " + cassandraTypesInTuple.get( i );
			fieldsWithDataTypes.add( str );
		}

		StringBuilder pks = new StringBuilder();
		for ( int i = 0; i < primaryKeys.length; i++ )
		{
			pks.append( primaryKeys[ i ] + "," );
		}
		pks.deleteCharAt( pks.length() - 1 );

		return "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ("
				+ StringUtils.join( fieldsWithDataTypes, "," ) + "," + "PRIMARY KEY("
				+ pks.toString() + ")) WITH COMPACT STORAGE";
	}

}
