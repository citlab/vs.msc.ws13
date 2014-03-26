package de.tu_berlin.citlab.storm.sinks;

import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.storm.builder.StreamBuilder;
import de.tu_berlin.citlab.storm.builder.StreamSink;
import de.tu_berlin.citlab.storm.operators.CassandraOperator;

public class CassandraSink extends StreamSink {
    public CassandraSink(StreamBuilder builder, CassandraConfig cassandraCfg ) {
        super(builder, new CassandraOperator(cassandraCfg) );
    }
}