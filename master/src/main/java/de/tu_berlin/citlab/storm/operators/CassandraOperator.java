package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.db.*;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;
import org.apache.commons.lang.StringUtils;

public class CassandraOperator implements IOperator {

    private boolean initialized = false;
    private boolean isCounterBolt = false;

    private CassandraDAO dao = new CassandraDAO();

    private CassandraConfig config;
    private Counter ctn;

    private Fields keyFields;


    public CassandraOperator( CassandraConfig config ){
        this.config = config;
        this.ctn = new Counter( config );
    }

    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) {

        System.out.println("debug: store "+tuples.size() );

        // First tuple used to initialize datastructures and derive data types
        if ( !initialized )
        {
            if( !config.isCounterBolt() ) {
                dao.init();
                dao.setConfig(config);
                dao.analyzeTuple( tuples.get(0) );
                dao.createDataStructures();
            }
            else {
                ctn.connect( config.getIP() );
                ctn.createDataStructures();

                keyFields = new Fields(config.getPrimaryKeys().getPrimaryKeyFields());

            }

            initialized = true;
        }

        try{
            if( !config.isCounterBolt() ) {

                for( Tuple t : tuples ){
                    System.out.println("debug: store "+t);
                }

                dao.store( tuples );
            } else {

                for( Tuple t : tuples ){

                    List<Object> keyValues = t.select( keyFields  );
                    List<Object> val = t.select(config.getTupleFields());

                    System.out.println("debug: store "+t);

                    ctn.update( keyValues, (int)val.get(0) );
                }//for
            }


        } catch (Exception e ){
            // ERROR
            System.err.print("ERROR: "+e);
            e.printStackTrace();
        }

        // emit tuples
        for( Tuple p : tuples ){
            collector.emit(p.getValues());
        }
    }
}
