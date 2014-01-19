package de.tu_berlin.citlab.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.tu_berlin.citlab.twitter.ConfiguredTwitterStreamBuilder;
import de.tu_berlin.citlab.twitter.InvalidTwitterConfigurationException;
import de.tu_berlin.citlab.twitter.TwitterConfiguration;

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8650869752517101545L;

	private SpoutOutputCollector collector;

	// The linked blocking queue is needed, as the offering in the list by the
	// twitter stream and the polling from the list by the spout is asynchronous
	private LinkedBlockingQueue<Status> queue = null;

	private TwitterStream twitterStream = null;

	private final TwitterConfiguration config;

	public TwitterSpout(TwitterConfiguration config)
			throws InvalidTwitterConfigurationException {
		if (!config.isValid()) {
			throw new InvalidTwitterConfigurationException(
					"The passed configuration is not valid");
		}
		this.config = config;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception e) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {

			}
		};

		twitterStream = ConfiguredTwitterStreamBuilder.getTwitterStream(config
				.getTwitterUser());
		twitterStream.addListener(listener);

		FilterQuery filterQuery = new FilterQuery();
		filterQuery.track(config.getKeywords());
		filterQuery.language(config.getLanguages());
		twitterStream.filter(filterQuery);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(createOutputValues(ret));
		}
	}

	private Values createOutputValues(Status ret) {

		final String[] outputFields = config.getOutputFields();
		Object[] values = new Object[outputFields.length];

		for (int i = 0; i < outputFields.length; i++) {
			switch (outputFields[i]) {
			case "user":
				values[i] = ret.getUser().getName();
				break;

			case "tweet":
				values[i] = ret.getText();
				break;

			case "date":
				values[i] = ret.getCreatedAt().getTime();
				break;

			case "lang":
				values[i] = ret.getIsoLanguageCode();
				break;

			case "geolocation":
				values[i] = ret.getGeoLocation();
				break;
				
			case "id":
				values[i] = ret.getId();
				break;

			default:
				break;
			}
		}

		return new Values(values);
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(config.getOutputFields()));
	}
}