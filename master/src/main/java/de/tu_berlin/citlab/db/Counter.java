package de.tu_berlin.citlab.db;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.apache.commons.lang.StringUtils;

public class Counter implements Serializable
{
	CassandraDAO dao;
	CassandraConfig config;
	String assembledCounterTableName;
	String counterName;
	String pkname;
	Cluster cluster;
	Session session;

	public Counter( CassandraConfig config )
	{
		this.config = config;
	}

	public void setConfig( CassandraConfig config )
	{
		this.config = config;
	}

	public void connect( String node )
	{
		cluster = Cluster.builder().addContactPoint( node ).withPort( 9042 ).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf( "Connected to cluster: %s\n", metadata.getClusterName() );
		for ( Host host : metadata.getAllHosts() )
		{
			System.out.printf( "Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(),
					host.getAddress(), host.getRack() );
		}
		session = cluster.connect();
	}

	// Create dedicated counter tables as needed in Cassandra for couting
	public void createDataStructures()
	{
		counterName = config.getTupleFields().get( 0 );
		pkname = config.getPrimaryKeys().getPrimaryKeyFields()[ 0 ];
		assembledCounterTableName = pkname + "_" + counterName;
		StringBuilder sb = new StringBuilder( "CREATE TABLE IF NOT EXISTS " );
		sb.append( config.getKeyspace() );
		sb.append( String.format( ".%s(%s text PRIMARY KEY,%s counter)", ""
				+ assembledCounterTableName, config.getPrimaryKeys().getPrimaryKeyFields()[ 0 ],
				config.getTupleFields().get( 0 ) ) );

		String createTableQuery = sb.toString();
		String createKeyspaceQuery =
				String.format(
						"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
						config.getKeyspace() );

		executeQuery( createKeyspaceQuery );
		executeQuery( createTableQuery );
	}

	// Increment or decrement the counter
	public void update( List <Object> keyValues, long number )
	{
		String keyspace_table = config.getKeyspace() + "." + assembledCounterTableName;
		String str_keyValues = StringUtils.join( keyValues, "-" );

		executeQuery( String.format( "UPDATE %s SET %s = %s + %d WHERE %s = '%s';", keyspace_table,
				counterName, counterName, number, pkname, str_keyValues ) );
	}

	public void executeQuery( String query )
	{
		try
		{
			session.execute( query );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}
}