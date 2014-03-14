package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.db.*;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

public class CassandraOperator implements IOperator {

    private boolean initialized = false;

    CassandraDAO dao = null;

    CassandraConfig config;


    public CassandraOperator( CassandraConfig config ){
        this.config = config;
        this.config.setIP("127.0.0.1");
        this.dao = new CassandraDAO();
        //this.config.setIP( loadClusterManagerIPFromProperties() );
        //System.out.println(loadClusterManagerIPFromProperties());
        //System.out.println("cluster ip: " + loadClusterManagerIPFromProperties());
    }

    public String loadClusterManagerIPFromProperties()
    {
    	Properties prop = new Properties();
    	InputStream in = getClass().getResourceAsStream("citstorm.properties");
    	try
		{
			prop.load( in );
			in.close();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
    	String clusterManagerIP =  prop.getProperty( "cluster-manager-ip" );
    	return getCassandraClusterIPFromClusterManager( clusterManagerIP );

     }
	
    private String getCassandraClusterIPFromClusterManager( String clusterManagerIP ) 
	{
		String USER_AGENT = "Mozilla/5.0";
		String url_string = "http://" + clusterManagerIP + ":9000/lookup?type=cassandra";
		StringBuffer sb = null;
		try {
			URL url = new URL(url_string);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestProperty("User-Agent", USER_AGENT);
			int responseCode = con.getResponseCode();  //TODO: check response code
			BufferedReader in = new BufferedReader( new InputStreamReader( con.getInputStream() ) );
			String inputLine;
			sb = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				sb.append(inputLine);
			}
			in.close();
		} catch (MalformedURLException e) {
			// TODO Automatisch generierter Erfassungsblock
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Automatisch generierter Erfassungsblock
			e.printStackTrace();
		}
 
		return sb.toString();
	}
    
    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) {

        // First tuple used to initialize datastructures and derive data types
        if ( !initialized )
        {

            dao.setConfig(config);
            dao.init();
            dao.analyzeTuple( tuples.get(0) );
            dao.createDataStructures();

            initialized = true;
        }
        else
        {
            try{
                dao.store( tuples );
            } catch (Exception e ){
                // ERROR
            }
        }

        // emit tuples
        for( Tuple p : tuples ){
            collector.emit(p.getValues());
        }
    }
}
