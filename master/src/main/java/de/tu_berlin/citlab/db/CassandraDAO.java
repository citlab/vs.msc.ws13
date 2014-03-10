package de.tu_berlin.citlab.db;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraDAO implements DAO, Serializable
{
	private Session session;
	private Cluster cluster;
	private PreparedStatement preparedStatement;
	public BoundStatement boundStatement;
	public BatchStatement batchStatement;
	public CassandraConfig config = null;
	public String createKeyspaceQuery;
	public String createTableQueryByFields;
	public TupleAnalyzer ta;

	public CassandraDAO()
	{

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

	public void createKeyspace( String query )
	{
		System.out.println( query );
		// session.execute( query );
	}

	public void createTable( String query )
	{
		System.out.println( query );
		// session.execute( query );
	}

	public void init()
	{
		if (config != null)
		{
			connect( config.getIP() );
		}
		else
		{
			connect( "127.0.0.1" );
		}
		
	}

    //TODO: do we need this function any more?
	public DBConfig createConfig()
	{
		this.config = new CassandraConfig();
		return config;
	}

    public void setConfig( CassandraConfig config ){
        this.config=config;
    }
	
	public void analyzeTuple( Tuple tuple )
	{
		ta = new TupleAnalyzer( tuple );
		ta.setPrimaryKey( config.getPrimaryKeys() );  //TODO: pk anderswo behandeln
		createKeyspaceQuery = ta.createKeyspaceQuery( config.getKeyspace() );
		createTableQueryByFields = ta.createTableQueryByFields( config.getKeyspace(), config.getTable(), ta.fieldsInTuple.toList() );
	}
	
	public void createDataStructures()
	{
		session.execute( createKeyspaceQuery );  //TODO: exeption handling
		session.execute( createTableQueryByFields );
	}

	public byte[] serializeObject( Object obj )
	{
		byte[] byteArray = {};
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try 
		{ 
			ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject( obj );
			out.close();
			byteArray = bos.toByteArray();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return byteArray;
	}
	
	public void store( List <Tuple> tuples )
	{

		
		setPreparedStatement( ta.createPreparedInsertStatement( config.getKeyspace(), config.getTable() ) );
		
		for ( Tuple tuple: tuples )
		{
			makeBatch();
			
			List <Object> values = new ArrayList <Object>();
			for ( int i = 0; i < ta.cassandraTypesInTuple.size(); i++ )
			{
				//System.out.println("javaTypesinTuple: " + ta.javaTypesInTuple);
				//System.out.println("switch: " + ta.javaTypesInTuple.get( i ));
//				switch( ta.javaTypesInTuple.get( i ) )  //TODO: besser enum mit typen verwenden
//				{
//					case "String":
//						values.add( tuple.getString( i ) );
//						break;
//					case "Integer":
//						values.add( tuple.getInteger( i ) );
//						break;
//					case "Long":
//						values.add( tuple.getLong( i ) );
//						break;
//					case "blob":  // Object must be serialized
//						values.add( ByteBuffer.wrap( serializeObject( tuple.getValue( i ) ) ) );
//						break;
//				}
			}
			bindValues( values.toArray( new Object[ values.size() ] ) );
			addToBatch( boundStatement );
			System.out.println("Values: " + values );
		}

		batchExecute();
	}

	public void makeBatch()
	{
		this.batchStatement = new BatchStatement();
	}

	public BoundStatement getBoundStatement()
	{
		return boundStatement;
	}

	public void addToBatch( BoundStatement boundStatement )
	{
		batchStatement.add( boundStatement );
	}

	public void batchExecute()
	{
		//System.out.println( batchStatement.toString() );
		session.execute( batchStatement );
	}

	public void bindValues( Object... objects )
	{
		assert this.preparedStatement != null: "Prepared Statement darf beim Binden nicht Null sein!";
		boundStatement = new BoundStatement( preparedStatement ).bind( objects );
	}

	public void setPreparedStatement( String query )
	{
		preparedStatement = this.session.prepare( query );
	}

}

/*
 * private Session session; private Cluster cluster; private String db; private
 * String table; private Map<String, String> tableFields; private List<Tuple>
 * tuples; private Map<String, String> fieldsFromTuple; private
 * PreparedStatement preparedStatement; public List<String> queries; public
 * BoundStatement boundStatement; public CassandraConfig config; public
 * BatchStatement batchStatement;
 */

/*
 * public BoundStatement getBoundStatement() { return boundStatement; }
 * 
 * public void addToBatch( BoundStatement boundStatement ) { batchStatement.add(
 * boundStatement ); }
 * 
 * public void batchExecute() { this.getSession().execute( batchStatement ); }
 * 
 * public void bindValues( Object... objects ) { assert this.preparedStatement
 * != null : "Prepared Statement darf beim Binden nicht Null sein!";
 * boundStatement = new BoundStatement( preparedStatement ).bind( objects ); }
 * 
 * public void makeBatch() { this.batchStatement = new BatchStatement(); }
 * 
 * public Session getSession() { return session; }
 * 
 * public void connect( String node ) { cluster =
 * Cluster.builder().addContactPoint( node ).build(); Metadata metadata =
 * cluster.getMetadata(); System.out.printf( "Connected to cluster: %s\n",
 * metadata.getClusterName() ); for ( Host host : metadata.getAllHosts() ) {
 * System.out.printf( "Datacenter: %s; Host: %s; Rack: %s\n",
 * host.getDatacenter(), host.getAddress(), host.getRack() ); } session =
 * cluster.connect(); }
 * 
 * public void setPreparedStatement( String query ) { preparedStatement =
 * this.session.prepare( query ); }
 * 
 * public void createDbFromConfig() { String strategy = config.storingStrategy;
 * if ( strategy.equalsIgnoreCase( "simple" ) ) { strategy = "SimpleStrategy"; }
 * // else if ..
 * 
 * int replicationFactor = config.replicationFactor;
 * 
 * Pattern pattern = Pattern.compile( "\\." ); String[] st = pattern.split(
 * config.query ); String db = st[ 0 ].split( " " )[ 2 ]; this.db = db;
 * INSTANCE.session .execute( String .format(
 * "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'%s', 'replication_factor':%d};"
 * , db, strategy, replicationFactor ) ); }
 * 
 * public void createTableFromConfig() { String strategy =
 * config.storingStrategy; int replicationFactor = config.replicationFactor;
 * 
 * Pattern pattern = Pattern.compile( "\\." ); String[] st = pattern.split(
 * config.query ); String table = st[ 1 ].split( " " )[ 0 ]; this.table = table;
 * 
 * String str = String.format( "CREATE TABLE IF NOT EXISTS %s.%s (", this.db,
 * this.table ); StringBuilder sb = new StringBuilder( str ); Set<String> keys =
 * config.dataTypes.keySet(); Iterator<String> it = keys.iterator(); while (
 * it.hasNext() ) { String key = it.next(); String value = ( String )
 * config.dataTypes.get( key ); sb.append( key + " " + value + "," ); }
 * sb.deleteCharAt( sb.length() - 1 ); sb.append( ") WITH COMPACT STORAGE" );
 * 
 * INSTANCE.session.execute( sb.toString() );
 */

