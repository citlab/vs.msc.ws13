package de.tu_berlin.citlab.storm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFStreamingBolt;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFTestTopology {
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt("map", new UDFStreamingBolt(new Fields("key", "value"), new Fields("key", "value"),
			new IOperator() {
				private static final long serialVersionUID = -4627321197468747477L;
				public Values[] execute(Values param) {
					String newKey = param.get(0) + " mapped";
					int newValue = myExistingFunction((Integer)param.get(1));
					return new Values[] { new Values(newKey, newValue) };
				}
				public int myExistingFunction(int param) {
					return param + 10;
				}
			}
		), 1).shuffleGrouping("spout");
		builder.setBolt("flatmap", new UDFStreamingBolt(new Fields("key", "value"), new Fields("key", "value"),
			new IOperator() {
				private static final long serialVersionUID = 6893429130604026037L;
				public Values[] execute(Values param) {
					Values[] result = new Values[2];
					String inputKey = (String)param.get(0);
					int inputValue = (Integer)param.get(1);
					result[0] = new Values(inputKey + "flatmapped1", myExistingFunction1(inputValue));
					result[1] = new Values(inputKey + "flatmapped2", myExistingFunction2(inputValue));
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
		builder.setBolt("filter", new UDFStreamingBolt(new Fields("value"), new Fields("value"),
			new FilterOperator(new FilterUDF() {
				private static final long serialVersionUID = -8778283315568184418L;
				public Boolean execute(Values param) {
					return (Integer)param.get(0) > 0;
				}
			})
		), 1).shuffleGrouping("flatmap");
//		builder.setBolt("reducer", new UDFBolt(new Fields("value"), new Fields("value"),
//			new IOperator() {
//				
//				public Values[] execute(Values param) {
//					// TODO Auto-generated method stub
//					return null;
//				}
//			}
//		), 1).fieldsGrouping("filter", new Fields("value"));
		
		Config conf = new Config();
		conf.setDebug(true);


		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("overflow-test", conf, builder.createTopology());
	}

}
