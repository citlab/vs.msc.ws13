package de.tu_berlin.citlab.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import de.tu_berlin.citlab.storm.operators.CassandraOperator;
import backtype.storm.tuple.Fields;

public class Test
{

	 public static String getCassandraClusterIPFromClusterManager( String clusterManagerIP ) 
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
	
	public static void main( String[] args ) throws Exception
	{
		String clusterManagerIP = "54.195.243.38";
		System.out.println(getCassandraClusterIPFromClusterManager(clusterManagerIP));
	}

}
