package de.tu_berlin.citlab.storm.spouts;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.db.CassandraDAO;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;

import java.util.Iterator;

abstract public class CassandraDataProviderSpout extends UDFSpout {

    private CassandraDAO dao = new CassandraDAO();
    private Iterator<Values> stored_data;
    private boolean ready=false;
    private CassandraConfig Cassandracfg;

    public CassandraDataProviderSpout(Fields outputFields, CassandraConfig cfg ) {
        super(outputFields);
        this.Cassandracfg = cfg;
    }

    @Override
    public void open(){
        // load data from cassandra
        dao.setConfig(this.Cassandracfg);
        dao.init();
        stored_data = dao.source( Cassandracfg.getKeyspace(), Cassandracfg.getTable(), outputFields ).findAll();
        ready=true;
    }
    @Override
    public void nextTuple(){
        if(ready){
            if(stored_data.hasNext() ){
                Values val = stored_data.next();
                getOutputCollector().emit( val );
            } else {
            }
        }
    }

    @Override
    public void close(){
    }
}
