package de.tu_berlin.citlab.twitter;

import java.util.Properties;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class ConfiguredTwitterStreamBuilder {

        public static TwitterStream getTwitterStream(Properties twitterUser) {

                String username = (String) twitterUser.get("username");
                String pwd = (String) twitterUser.get("password");
                String oAuthConsumerKey = (String) twitterUser.get("oAuthConsumerKey");
                String oAuthConsumerSecret = (String) twitterUser
                                .get("oAuthConsumerSecret");
                String oAuthAccessToken = (String) twitterUser.get("oAuthAccessToken");
                String oAuthAccessTokenSecret = (String) twitterUser
                                .get("oAuthAccessTokenSecret");

                TwitterStreamFactory fact = new TwitterStreamFactory(
                                new ConfigurationBuilder().setUser(username).setPassword(pwd)
                                                .setOAuthConsumerKey(oAuthConsumerKey)
                                                .setOAuthConsumerSecret(oAuthConsumerSecret)
                                                .setOAuthAccessToken(oAuthAccessToken)
                                                .setOAuthAccessTokenSecret(oAuthAccessTokenSecret)
                                                .build());
                return fact.getInstance();
        }
}