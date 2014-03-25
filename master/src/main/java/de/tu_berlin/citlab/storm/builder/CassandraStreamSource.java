package de.tu_berlin.citlab.storm.builder;


import backtype.storm.tuple.Fields;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.storm.spouts.CassandraDataProviderSpout;
import de.tu_berlin.citlab.storm.spouts.UDFSpout;
import de.tu_berlin.citlab.storm.udf.UDFOutput;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;

public class CassandraStreamSource extends StreamSource {
    private UDFSpout spout;
    private CassandraConfig cassandraConfig;

    public CassandraStreamSource(StreamBuilder builder, CassandraConfig cassandraConfig, Fields output )
            throws InvalidTwitterConfigurationException {
        super(builder);
        this.cassandraConfig = cassandraConfig;

        spout = new CassandraDataProviderSpout( output, cassandraConfig );

        getStreamBuilder().getTopologyBuilder().setSpout(getNodeId(), spout );
    }

    @Override
    public UDFOutput getUDFOutput(){
        return spout;
    }
}
