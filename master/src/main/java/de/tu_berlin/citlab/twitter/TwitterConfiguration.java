/**
 * A simple container class for holding all information needed to configure the twitter spout
 */

package de.tu_berlin.citlab.twitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TwitterConfiguration implements Serializable {

        private static final long serialVersionUID = 4888981421926856894L;

        // a list of valid field names for the output tuple
        private static final List<String> validOutputFields = new ArrayList<String>(
                        Arrays.asList(new String[] { "user", "tweet", "date", "lang",
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
                        return false;
                }

                if (twitterUser.get("username") == null
                                || twitterUser.get("password") == null
                                || twitterUser.get("oAuthConsumerKey") == null
                                || twitterUser.get("oAuthConsumerSecret") == null
                                || twitterUser.get("oAuthAccessToken") == null
                                || twitterUser.get("oAuthAccessTokenSecret") == null) {
                        return false;
                }
                if (keywords == null || keywords.length == 0) {
                        return false;
                }

                if (outputFields == null || outputFields.length == 0) {
                        return false;
                }

                for (String s : outputFields) {
                        if (!validOutputFields.contains(s)) {
                                return false;
                        }
                }

                return true;
        }
}