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
		{
			put( "String", "text" );
			put( "Integer", "int" );
			put( "Long", "bigint" );
		}
	};

	public TupleAnalyzer( Tuple tuple )
	{
		// this.keyspace = keyspace;
		// this.table = table;
		fieldsInTuple = tuple.getFields();
		objectsInTuple = tuple.getValues();
		System.out.println(objectsInTuple);
		getJavaAndCassandraTypesFromTupleObjects( objectsInTuple );

		//CassandraBolt.dao.createKeyspace( createKeyspaceQuery( keyspace ) );
		//CassandraBolt.dao.createTable( createTableQueryByFields( keyspace, table,
		//		fieldsInTuple.toList() ) );
	}
	
	public void setPrimaryKey( PrimaryKey pk )
	{
		this.primaryKeys = pk.getPrimaryKeyFields();
	}

	public void getJavaAndCassandraTypesFromTupleObjects( List <Object> objects )
	{
		for ( Object obj : objects )
		{
			String classname = getClassName( obj );
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
	
	public String getClassName( Object obj )
	{
		String canonical = obj.getClass().getCanonicalName();
		String[] parts = canonical.split( "\\." );
		String classname = parts[ parts.length - 1 ];
		return classname;
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
		// <value1> <datatype1> ...
		List <String> fieldsWithDataTypes = new ArrayList <String>();
		for ( int i = 0; i < fields.size(); i++ )
		{
			
			String str = fields.get( i ) + " " + cassandraTypesInTuple.get( i );
			fieldsWithDataTypes.add( str );
			
/*			// testen, ob primitiver Typ oder String in javaTypesInTuple
			if ( javaToCassandraTypes.containsKey( javaTypesInTuple.get( i ) ) )
			{
				String str = fields.get( i ) + " " + javaTypesInTuple.get( i );
				fieldsWithDataTypes.add( str );
			}
			else // Referenztyp
			{
				String str = fields.get( i ) + " blob";
				fieldsWithDataTypes.add( str );
			}*/

		}

		StringBuilder pks = new StringBuilder();
		for ( int i = 0; i < primaryKeys.length; i++ )
		{
			pks.append( primaryKeys[ i ] + "," );
		}
		pks.deleteCharAt( pks.length() - 1 );

		// CREATE TABLE <table>(<value1> <datatype1>, <value2> <datatype2> ...,
		// PRIMARY KEYS(<value1>, <value2> ...)
		return "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ("
				+ StringUtils.join( fieldsWithDataTypes, "," ) + "," + "PRIMARY KEY("
				+ pks.toString() + ")) WITH COMPACT STORAGE";
	}

}
