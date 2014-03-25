package de.tu_berlin.citlab.storm.builder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;

import java.io.Serializable;
import java.util.List;

public class StreamBuilder implements Serializable {
    private TopologyBuilder topology;
    private Window<Tuple, List<Tuple>> windowType;

    public StreamBuilder(){
        topology = new TopologyBuilder();
    }
    public Window<Tuple, List<Tuple>> getDefaultWindowType(){ return windowType; }
    public TopologyBuilder getTopologyBuilder(){
        return topology;
    }

    public TopologyBuilder setDefaultWindowType( Window<Tuple, List<Tuple>> window ){
        windowType = window;
        return topology;
    }
}
