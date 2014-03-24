package de.tu_berlin.citlab.db;

import java.util.List;
import java.util.ArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;

import backtype.storm.tuple.Values;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class TestSelect
{
	static Cluster cluster;
	static Session session;

	public static void main( String[] args ) throws Exception
	{

		TestSelect ts = new TestSelect();
		ts.connect( "127.0.0.1" );
		Statement st = new SimpleStatement( "SELECT * FROM myks.tab1" );

		st.setFetchSize( 10 );
		ResultSet rs = session.execute( st );

		int i = 0;
		for ( Row row : rs )
		{
			System.out.println( "Iterations : " + i );
			i++;
		}

		CassandraIterator citer = new CassandraIterator( rs );
		session.shutdown();
	}

	public void connect( String node )
	{
		cluster = Cluster.builder().addContactPoint( node ).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf( "Connected to cluster: %s\n", metadata.getClusterName() );
		for ( Host host : metadata.getAllHosts() )
		{
			System.out.printf( "Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(),
					host.getAddress(), host.getRack() );
		}
		session = cluster.connect();
	}

	public void shutdown()
	{
		session.shutdown();
	}

}
