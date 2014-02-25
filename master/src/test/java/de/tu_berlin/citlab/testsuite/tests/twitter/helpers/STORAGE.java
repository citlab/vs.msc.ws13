package de.tu_berlin.citlab.testsuite.tests.twitter.helpers;

import java.util.HashMap;

/**
 * Created by Constantin (on behalf of Kay) on 1/22/14.
 */
public final class STORAGE
{
    public static HashMap<String, User> users = new HashMap<String, User>();
    public static HashMap<String, BadWord> badWords = new HashMap<String, BadWord>();

    public static void updateUser( String userId, String word, int significance ){
        if( users.containsKey(userId) ){
            users.get(userId).updateWord( word, significance);
        } else{
            User u = new User(userId, 0);
            u.updateWord( word, significance);
            users.put(userId, u );
        }
    }
}
