/**
 * A simple container class for holding all information needed to configure the twitter spout
 */

package de.tu_berlin.citlab.twitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TwitterConfiguration implements Serializable {

        private static final long serialVersionUID = 4888981421926856894L;
        private static final Logger log = LogManager.getLogger(TwitterConfiguration.class); 

        // a list of valid field names for the output tuple
        private static final List<String> validOutputFields = new ArrayList<String>(
                Arrays.asList(new String[] { "user", "tweet", "date", "lang", "user_id", "tweet_id",
                        "geolocation" }));


        private final Properties twitterUser;
        private final String[] keywords;
        private final String[] languages;
        private final String[] outputFields;

        // The twitter user must be passed as Properties object, as the loading on
        // the node would fail, because the config file is not present.
        public TwitterConfiguration(Properties twitterUser, String[] keywords,
                        String[] languages, String[] outputFields) {
                this.twitterUser = twitterUser;
                this.keywords = keywords;
                this.languages = languages;
                this.outputFields = outputFields;
        }

        public TwitterConfiguration(Properties twitterUser, String[] keywords,
                        String[] outputFields) {
                this.twitterUser = twitterUser;
                this.keywords = keywords;
                this.languages = null;
                this.outputFields = outputFields;
        }

        public Properties getTwitterUser() {
                return twitterUser;
        }

        public String[] getKeywords() {
                return keywords;
        }

        public String[] getLanguages() {
                return languages;
        }

        public String[] getOutputFields() {
                return outputFields;
        }

        /**
         * Checks, if the created twitter configuration is valid. Result will be
         * false, if one of the following points apply: - User is null - User has
         * not set all the required fields - Keywords is either empty or null -
         * OutputFields is either emtpy, null or contains invalid field names
         * 
         * @return
         */
        public boolean isValid() {
                if (twitterUser == null) {
                        log.warn("properties representation of auth credentials for twitter is null");
                        return false;
                }

                if (twitterUser.get("username") == null
                                || twitterUser.get("password") == null
                                || twitterUser.get("oAuthConsumerKey") == null
                                || twitterUser.get("oAuthConsumerSecret") == null
                                || twitterUser.get("oAuthAccessToken") == null
                                || twitterUser.get("oAuthAccessTokenSecret") == null) {
                        log.warn("at least one of the fields 'username', 'password', 'oAuthConsumerKey', 'oAuthConsumerSecret', 'oAuthAccessToken' or 'oAuthAccessTokenSecret' of the twitter config is null");
                        return false;
                }
                if (keywords == null || keywords.length == 0) {
                        log.warn("keywords are null or empty");
                        return false;
                }

                if (outputFields == null || outputFields.length == 0) {
                        log.warn("outputFields are null or empty");
                        return false;
                }

                for (String s : outputFields) {
                        if (!validOutputFields.contains(s)) {
                                log.warn("outputfield '%s' is not valid", s);
                                return false;
                        }
                }

                return true;
        }
}