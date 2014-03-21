package de.tu_berlin.citlab.storm.topologies;


import backtype.storm.generated.StormTopology;
import java.io.Serializable;


public interface TopologyCreation extends Serializable
{
   public StormTopology createTopology();
}
