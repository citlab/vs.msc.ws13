package de.tu_berlin.citlab.storm.sinks;

import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.storm.builder.StreamSink;
import de.tu_berlin.citlab.storm.operators.CassandraOperator;
import de.tu_berlin.citlab.storm.operators.SinkOperator;

public class CassandraSink extends SinkOperator {
        /*
        super( cassandraCfg.getTupleFields(),
                new CassandraOperator(cassandraCfg)  );*/
}