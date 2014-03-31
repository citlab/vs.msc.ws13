package de.tu_berlin.citlab.storm.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import de.tu_berlin.citlab.storm.helpers.StringHelper;
import de.tu_berlin.citlab.testsuite.helpers.LogPrinter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class TwitterSpout extends UDFSpout {

	private static final long serialVersionUID = 8650869752517101545L;
	private static final Logger LOGGER = LogManager.getLogger("Spout");


	// The linked blocking queue is needed, as the offering in the list by the
	// twitter stream and the polling from the list by the spout is asynchronous
	private LinkedBlockingQueue<Status> queue = null;

	private TwitterStream twitterStream = null;

	private final TwitterConfiguration config;

	public TwitterSpout(TwitterConfiguration config )
					throws InvalidTwitterConfigurationException {
        super(new Fields(config.getOutputFields()));
        if (!config.isValid()) {
					throw new InvalidTwitterConfigurationException(
									"The passed configuration is not valid");
			}
			this.config = config;
	}

	@Override
	public void open() {
			queue = new LinkedBlockingQueue<Status>(1000);
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

			LOGGER.info("Opened Twitter Stream!");
	}


    @Override
	public void nextTuple() {
			Status ret = queue.poll();
			if (ret == null) {
					Utils.sleep(50);
			} else {
					Values outputVals = createOutputValues(ret);
					LOGGER.info("Emitting twitter tuple Values: {}", LogPrinter.toValString(outputVals));
					collector.emit(outputVals);
			}
	}
	
	private Values createOutputValues(Status ret) {
		final String[] outputFields = config.getOutputFields();
		Object[] values = new Object[outputFields.length];
		
                for (int i = 0; i < outputFields.length; i++) {
                        if( outputFields[i].compareTo("user") == 0)
                            values[i] = StringHelper.removeAllNonBMPCharacters(ret.getUser().getName());
                        if( outputFields[i].compareTo("tweet") == 0)
                            values[i] = StringHelper.removeAllNonBMPCharacters(ret.getText());
                        if( outputFields[i].compareTo("date") == 0)
                            values[i] = ret.getCreatedAt().getTime();
                        if( outputFields[i].compareTo("lang") == 0)
                            values[i] = StringHelper.removeAllNonBMPCharacters(ret.getIsoLanguageCode());
                        if( outputFields[i].compareTo("geolocation") == 0)
                            values[i] = ret.getGeoLocation();
                        if( outputFields[i].compareTo("id") == 0)
                            values[i] = ret.getUser().getId();
                        if( outputFields[i].compareTo("user_id") == 0)
                            values[i] = ret.getUser().getId();
                        if( outputFields[i].compareTo("tweet_id") == 0)
                            values[i] = ret.getId();
                }//for

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

    @Override
    public Fields getOutputFields() {
        return new Fields(config.getOutputFields());
    }
}