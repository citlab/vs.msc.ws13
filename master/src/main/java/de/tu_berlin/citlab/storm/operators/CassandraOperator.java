package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.db.*;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;

public class CassandraOperator extends IOperator {

    private boolean initialized = false;

    private CassandraDAO dao = new CassandraDAO();

    private CassandraConfig config;
    private Counter ctn;

    private Fields keyFields;

    public CassandraOperator( CassandraConfig config ){
        this.config = config;
        
        if(this.config.isCounterBolt() ){
            this.ctn = new Counter( config );
        }
    }

    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) throws OperatorException {
        // First tuple used to initialize datastructures and derive data types
        if ( !initialized )
        {
            if( !config.isCounterBolt() ) {
                dao.setConfig(config);
                dao.init();
                dao.analyzeTuple( tuples.get(0) );
                dao.createDataStructures();
                dao.prepare();

            }
            else {
                keyFields = new Fields(config.getPrimaryKeys().getPrimaryKeyFields());

                ctn.connect( config.getIP() );
                ctn.createDataStructures();

            }

            initialized = true;
        }

        try{
            if( !config.isCounterBolt() ) {

                for( Tuple t : tuples ){
                    this.getUDFBolt().log_debug("cassandra-operator", "store " + t);
                }

                synchronized (this){
                    dao.store( tuples );
                }
           } else {

                for( Tuple t : tuples ){
                    List<Object> keyValues = t.select( keyFields  );
                    List<Object> val = t.select(config.getTupleFields());

                    this.getUDFBolt().log_debug("cassandra-operator", "store " + t);

                    synchronized (this){
                        ctn.update( keyValues, (Long)val.get(0) );
                    }
                }//for
            }


        } catch (Exception e ){
            this.getUDFBolt().log_error("Storing of tuples into Cassandra DB failed!", e );

            e.printStackTrace();

			throw new OperatorException("Storing of tuples into Cassandra DB failed!");
        }

        // emit tuples
        for( Tuple p : tuples ){
            collector.emit(p.getValues());
        }
    }

}
