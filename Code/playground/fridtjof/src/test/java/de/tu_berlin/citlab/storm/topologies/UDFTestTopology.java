package de.tu_berlin.citlab.storm.topologies;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFTestTopology {
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt("map", new UDFBolt(new Fields("key", "value"), new Fields("key", "value"),
			new IOperator() {
				private static final long serialVersionUID = -4627321197468747477L;
				public List<List<Object>> execute(List<List<Object>> param) {
					String newKey = param.get(0).get(0) + " mapped";
					int newValue = myExistingFunction((Integer)param.get(0).get(1));
					List<List<Object>> result = new ArrayList<List<Object>>(1);
					result.add(new Values(newKey, newValue));
					return result;
				}
				public int myExistingFunction(int param) {
					return param + 10;
				}
			}
		), 1).shuffleGrouping("spout");
		builder.setBolt("flatmap", new UDFBolt(new Fields("key", "value"), new Fields("key", "value"),
			new IOperator() {
				private static final long serialVersionUID = 6893429130604026037L;
				public List<List<Object>> execute(List<List<Object>> param) {
					List<List<Object>> result = new ArrayList<List<Object>>();
					String inputKey = (String)param.get(0).get(0);
					int inputValue = (Integer)param.get(0).get(1);
					result.add(new Values(inputKey + "flatmapped1", myExistingFunction1(inputValue)));
					result.add(new Values(inputKey + "flatmapped2", myExistingFunction2(inputValue)));
					return result;
				}
				private int myExistingFunction1(int param) {
					return param *= 10;
				}
				private int myExistingFunction2(int param) {
					return param *= -1;
				}
			}
		), 1).shuffleGrouping("map");
		builder.setBolt("filter", new UDFBolt(new Fields("value"), new Fields("value"),
			new FilterOperator(new FilterUDF() {
				private static final long serialVersionUID = -8778283315568184418L;
				public Boolean execute(List<Object> param) {
					return (Integer)param.get(0) > 0;
				}
			})
		), 1).shuffleGrouping("flatmap");
		builder.setBolt("reducer", new UDFBolt(new Fields("value"), new Fields("value"),
			new IOperator() {
				private static final long serialVersionUID = 7655644319650809859L;
				public List<List<Object>> execute(List<List<Object>> param) {
					int reduced = 0;
					List<List<Object>> result = new ArrayList<List<Object>>(0);
					for(List<Object> tupel : param) {
						reduced += (Integer) tupel.get(0);
					}
					result.add(new Values(reduced));
					return result;
				}
			}, 2
		), 1).fieldsGrouping("filter", new Fields("value"));
		
		Config conf = new Config();
		conf.setDebug(true);


		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("overflow-test", conf, builder.createTopology());
	}

}
