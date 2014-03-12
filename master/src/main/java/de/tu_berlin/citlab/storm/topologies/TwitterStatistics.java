package de.tu_berlin.citlab.storm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.db.CassandraConfig;
import de.tu_berlin.citlab.db.PrimaryKey;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.operators.*;
import de.tu_berlin.citlab.storm.operators.join.StaticHashJoinOperator;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.spouts.TwitterSpout;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TimeWindow;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;
import de.tu_berlin.citlab.twitter.TwitterUserLoader;

import java.io.Serializable;
import java.util.*;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class TwitterStatistics implements Serializable {
	
	private final static Logger log = Logger.getLogger(TwitterStatistics.class);
	
	private static final int windowSize = 10;
	private static final int slidingOffset = 10;

	public Window<Tuple, List<Tuple>> COUNT_WINDOW = new CountWindow<Tuple>(
			windowSize, slidingOffset);

	// public Window<Tuple, List<Tuple>> TIME_WINDOW =new
	// CountWindow<Tuple>(windowSize, slidingOffset);

	public TwitterSpout createTwitterSpout() throws Exception {
		// Setup up Twitter configuration
		Properties user = TwitterUserLoader.loadUser("twitter.config");
		String[] keywords = new String[] { "der", "die", "das" };
		String[] languages = new String[] { "de" };
		String[] outputFields = new String[] { "user", "id", "tweet" };
		TwitterConfiguration config = new TwitterConfiguration(user, keywords,
				languages, outputFields);
		return new TwitterSpout(config);
	}

	public UDFBolt createCassandraSink() {

		CassandraConfig cassandraCfg = new CassandraConfig();
		cassandraCfg.setParams(
				// optional, but defaults not always sensable
				"127.0.0.1", "citstorm", "tweets",
				new PrimaryKey("user", "id"), /* CassandraFactory.PrimaryKey(..) */
				new Fields());

		return new UDFBolt(new Fields("user", "id", "tweet"),
				new CassandraOperator(cassandraCfg), COUNT_WINDOW);
	}

	public UDFBolt flatMapTweetWords() {
		return new UDFBolt(new Fields("user", "id", "word"), new IOperator() {

			@Override
			public void execute(List<Tuple> tuples, OutputCollector collector) {
				for (Tuple p : tuples) {
					String[] words = p.getStringByField("tweet").split(" ");
					for (String word : words) {
						collector.emit(new Values(p.getValueByField("user"), p
								.getValueByField("id"), word.trim()
								.toLowerCase()));
					}// for
				}// for
			}// execute()
		});
	}

	private UDFBolt createFilterHashTagsBolt() {
		return new UDFBolt(
			new Fields("hashtag"),
			new IOperator() {
				@Override
				public void execute(List<Tuple> input, OutputCollector collector) {
					for(Tuple t : input) {
						String word = t.getStringByField("word");
						if(word.startsWith("#")) {
							collector.emit(new Values(word));
//							log.error(word);
						}
					}
				}
			}
		);
	}
	
	private UDFBolt createFilterConnectorBolt() {
		return new UDFBolt(
			new Fields("connector"),
			new IOperator() {
				@Override
				public void execute(List<Tuple> input, OutputCollector collector) {
					for(Tuple t : input) {
						String word = t.getStringByField("word");
						if(word.startsWith("@")) {
							collector.emit(new Values(word));
//							log.error(word);
						}
					}
				}
			}
		);
	}
	

	private UDFBolt createHashtagCountsBolt() {
		return new UDFBolt(
			new Fields("hashtag", "count"),
			new IOperator() {
				@Override
				public void execute(List<Tuple> input, OutputCollector collector) {
					int count = 0;
					String hashtag = input.get(0).getStringByField("hashtag");
					for(Tuple _ : input) {
						count++;
					}
					collector.emit(new Values(hashtag, count));
//					log.error("hashtag '"+hashtag+"' '"+count+"' mal");
				}
			},
			new TimeWindow<Tuple>(10*1000, 2*1000),
			KeyConfigFactory.DefaultKey(), // no window-groups
			KeyConfigFactory.ByFields("hashtag")
		);
	}
	


	private IRichBolt createHashtagRankingsBolt() {
		return new UDFBolt(
			
			new Fields("hashtag_ranking"),
			new IOperator() {
				@Override
				public void execute(List<Tuple> input, OutputCollector collector) {
					int count = 0;
					String hashtag = input.get(0).getStringByField("hashtag");
					for(Tuple _ : input) {
						count++;
					}
					collector.emit(new Values(hashtag, count));
				}
			},
			new TimeWindow<Tuple>(10*1000, 2*1000),
			KeyConfigFactory.DefaultKey(), // no window-groups
			KeyConfigFactory.ByFields("count")
		);
	}

	public StormTopology createTopology() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// provide twitter streaminh data
		builder.setSpout("tweets", createTwitterSpout(), 1);

		builder.setBolt("words", flatMapTweetWords(), 1)
			.shuffleGrouping("tweets");
		
		builder.setBolt("hashtags", createFilterHashTagsBolt(), 1)
			.shuffleGrouping("words");
		
		builder.setBolt("hashtagCounts", createHashtagCountsBolt(), 1)
			.fieldsGrouping("hashtags", new Fields("hashtag"));
		
//		builder.setBolt("hashtagRanking", createHashtagRankingsBolt(), 1)
//			.globalGrouping("hashtagCounts");
		
		builder.setBolt("connector", createFilterConnectorBolt(), 1)
			.shuffleGrouping("words");


		return builder.createTopology();
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		conf.setDebug(false);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("twitter-statistics", conf,
				new TwitterStatistics().createTopology());
	}
}