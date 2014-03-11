package de.tu_berlin.citlab.db;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import de.tu_berlin.citlab.storm.operators.CassandraOperator;
import backtype.storm.tuple.Fields;

public class Test
{

	public static void main( String[] args ) throws Exception
	{
		String clusterManagerIP = "127.0.0.1";
		String USER_AGENT = "Mozilla/5.0";
		String url_string = "http://" + clusterManagerIP + ":8080/lookup?type=cassandra";
		URL url = new URL(url_string);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestProperty("User-Agent", USER_AGENT);
		int responseCode = con.getResponseCode();  //TODO: check response code
 		BufferedReader in = new BufferedReader( new InputStreamReader( con.getInputStream() ) );
		String inputLine;
		StringBuffer sb = new StringBuffer();
 		while ((inputLine = in.readLine()) != null) {
			sb.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(sb.toString());
		
		
		
		
		
/*		CassandraConfig cassandraCfg = new CassandraConfig();

		cassandraCfg.setParams( // optional, but defaults not always sensable
				"myks",
				"new2", 
				new PrimaryKey( "user", "id" ), 
				new Fields() 
		);
		CassandraOperator co = new CassandraOperator(cassandraCfg);*/

	}

}
