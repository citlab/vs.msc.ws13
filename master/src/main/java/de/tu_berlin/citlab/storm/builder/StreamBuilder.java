package de.tu_berlin.citlab.storm.builder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;

public class StreamBuilder {
    private TopologyBuilder topology;

    public StreamBuilder(){
        topology = new TopologyBuilder();
    }

    public TopologyBuilder getTopologyBuilder(){
        return topology;
    }

    public TopologyBuilder setDefaultWindow(){
        return topology;
    }
}
