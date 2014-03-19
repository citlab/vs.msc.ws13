package de.tu_berlin.citlab.storm.topologies;


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import java.io.Serializable;


/**
 * Created by Constantin on 19.03.2014.
 */
public interface TopologyCreation extends Serializable
{
   public Config getConfig();
   public StormTopology createTopology();
}
