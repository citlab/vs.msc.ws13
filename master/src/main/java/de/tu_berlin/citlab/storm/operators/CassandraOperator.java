package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.db.*;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;

public class CassandraOperator implements IOperator {

    private boolean initialized = false;

    CassandraDAO dao = new CassandraDAO();

    CassandraConfig config;


    public CassandraOperator( CassandraConfig config ){
        /* place your code here */
        this.config = config;
    }

    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) {

        System.out.println("---- store "+tuples.size() );

        // First tuple used to initialize datastructures and derive data types
        if ( !initialized )
        {
            dao.init();
            dao.setConfig(config);
            dao.analyzeTuple( tuples.get(0) );
            dao.createDataStructures();

            initialized = true;
        }

        try{
            dao.store( tuples );
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
