package de.tu_berlin.citlab.db;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


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
		this.config.setIP( "127.0.0.1" );
		//this.dao = new CassandraDAO();
        //dao.setConfig(this.config);
        //dao.init();
		connect( config.getIP() );
        createDataStructures();
	}
	public Counter()
	{
		//System.out.println("NEWWWWW COUNNNTEREERRR!!!!!");
		
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
	
	public void createDataStructures()
	{
		counterName = config.getTupleFields().get( 0 );
		pkname = config.getPrimaryKeys().getPrimaryKeyFields()[0];
		assembledCounterTableName = pkname+"_"+counterName;
		StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		sb.append( config.getKeyspace() );
		sb.append( String.format( ".%s(%s text PRIMARY KEY,%s counter)", 
				""+assembledCounterTableName,
				config.getPrimaryKeys().getPrimaryKeyFields()[0],
				config.getTupleFields().get( 0 ) ) );

		String createTableQuery = sb.toString();
		String createKeyspaceQuery = String.format(
				"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
				config.getKeyspace() );
		
		executeQuery( createKeyspaceQuery );
		executeQuery( createTableQuery );
	}
	
	public void update ( String key, int number )
	{
		String keyspace_table = config.getKeyspace() + "." + assembledCounterTableName;
		executeQuery( String.format( "UPDATE %s SET %s = %s + %d WHERE %s = '%s';", keyspace_table, counterName, counterName, number, pkname, key ));
		//System.out.println(String.format( "UPDATE %s SET %s = %s + %d WHERE %s = '%s';", keyspace_table, counterName, counterName, number, pkname, key ));
	}
	
	public void executeQuery( String query )
	{
		try
		{
			session.execute( query );
		}
		catch ( Exception e )
		{
			// TODO Automatisch generierter Erfassungsblock
			e.printStackTrace();
		}
	}
	

}
