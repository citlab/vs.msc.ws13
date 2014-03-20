
package de.tu_berlin.citlab.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import backtype.storm.tuple.Fields;

public class CassandraConfig implements DBConfig, Serializable
{
    private static final long serialVersionUID = 1L;

    private String IP;
    private PrimaryKey primaryKeys;
    private Fields tupleFields;
    private String keyspace;
    private String table;
    private boolean isCounterBolt=false;

    public void setParams( String keyspace, String table, PrimaryKey primaryKeys, Fields tupleFields ){
        setParams( keyspace, table, primaryKeys, tupleFields, false );
    }

    public void setParams( String keyspace, String table, PrimaryKey primaryKeys, Fields tupleFields, boolean isCntBolt )
    {
        this.primaryKeys = primaryKeys;
        this.tupleFields = tupleFields;
        this.keyspace = keyspace;
        this.table = table;
        this.isCounterBolt = isCntBolt;
    }

    public void setParams( String keyspace, String table ){
        setParams( keyspace, table, null, null, false );
    }

    static public String getCassandraClusterIPFromClusterManager()
	{
		String USER_AGENT = "Mozilla/5.0";
		String url_string = "http://citstorm.dd-dns.de:9000/lookup?type=cassandra";
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


    /**
     * @return keyspace
     */
    public String getKeyspace()
    {
        return keyspace;
    }


    /**
     * @return isCounterBolt
     */
    public boolean isCounterBolt(){
        return isCounterBolt;
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