package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.db.*;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

public class CassandraOperator implements IOperator {

    private boolean initialized = false;

    CassandraDAO dao = new CassandraDAO();

    CassandraConfig config;


    public CassandraOperator( CassandraConfig config ){
        this.config = config;
        this.getClusterIP();
    }

    public void getClusterIP()
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
    	this.config.setIP( prop.getProperty( "cluster-manager-ip" ) );
    	
    }
    
    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) {

        // First tuple used to initialize datastructures and derive data types
        if ( !initialized )
        {
            dao.init();
            dao.setConfig(config);
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
