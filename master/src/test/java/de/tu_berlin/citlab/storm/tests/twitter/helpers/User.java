package de.tu_berlin.citlab.storm.tests.twitter.helpers;

import java.util.HashMap;

/**
 * Created by Constantin (on behalf of Kay) on 1/22/14.
 */
public class User
{
    public static HashMap<String, Integer> wordStaticstics = new HashMap<String, Integer>();
    public String userId;
    public int total_significance;
    public User(String u,int s){
        userId=u;
        total_significance=s;
    } //User
    public void updateWord(String w, int significance){
        this.total_significance += significance;
        if( wordStaticstics.containsKey(w) ){
            wordStaticstics.put( w, wordStaticstics.get(w).intValue() + 1 );
        } else {
            wordStaticstics.put( w, 1 );
        }
    }
}
