package de.tu_berlin.citlab.storm.topologies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFWindowBolt;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IBatchOperator;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFTestTopology {
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		
		builder.setBolt("map", new UDFWindowBolt<String>(new Fields("key", "value"), new Fields("key", "value"), 1,
			new IBatchOperator<String>()
			{
				private static final long serialVersionUID = 1L;

				public List<Values[]> execute_batch(HashMap<String, Iterator<Values>> entryMap) 
				{
					List<Values[]> returnVals = new ArrayList<Values[]>();
					Iterator<Values> paramIterator = entryMap.get("all");
					
					while(paramIterator.hasNext()){
						Values param = paramIterator.next();
						
						String newKey = param.get(0) + " mapped";
						int newValue = myExistingFunction((Integer)param.get(1));
						
						returnVals.add(new Values[] { new Values(newKey, newValue) });
					}
					return returnVals;
				}
				private int myExistingFunction(int param) 
				{
					return param + 10;
				}

				public String sortBy_winKey(Values param) 
				{
					String key = "all";
					return key;
				}
			
			}
		), 1).shuffleGrouping("spout");
		
		builder.setBolt("flatmap", new UDFWindowBolt<String>(new Fields("key", "value"), new Fields("key", "value"), 1,
			new IBatchOperator<String>() 
			{
				private static final long serialVersionUID = 1L;

				public List<Values[]> execute_batch(HashMap<String, Iterator<Values>> entryMap) 
				{
					List<Values[]> returnVals = new ArrayList<Values[]>();
					Iterator<Values> paramIterator = entryMap.get("all");
					
					while(paramIterator.hasNext()){
						Values param = paramIterator.next();
						Values[] result = new Values[2];
						String inputKey = (String)param.get(0);
						int inputValue = (Integer)param.get(1);
						result[0] = new Values(inputKey + "flatmapped1", myExistingFunction1(inputValue));
						result[1] = new Values(inputKey + "flatmapped2", myExistingFunction2(inputValue));
						
						returnVals.add(result);
					}
					return returnVals;
				}
				private int myExistingFunction1(int param) 
				{
					return param *= 10;
				}
				private int myExistingFunction2(int param) 
				{
					return param *= -1;
				}
					
				public String sortBy_winKey(Values param) 
				{
					String key = "all";
					return key;
				}
			}
		), 1).shuffleGrouping("map");
		
//		builder.setBolt("filter", new UDFStreamingBolt(new Fields("value"), new Fields("value"), 1,
//			new FilterOperator(new FilterUDF() {
//				private static final long serialVersionUID = 1L;
//				public Boolean execute(Values param) {
//					return (Integer)param.get(0) > 0;
//				}
//			})
//		), 1).shuffleGrouping("flatmap");
		
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
