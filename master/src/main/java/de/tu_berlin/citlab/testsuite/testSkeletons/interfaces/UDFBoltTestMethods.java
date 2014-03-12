package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.Window;

import java.util.List;

/**
 * Created by Constantin on 1/20/14.
 */
public interface UDFBoltTestMethods
{
    public List<Tuple> generateInputTuples();

    public Window<Tuple, List<Tuple>> initWindow();

    public IKeyConfig initKeyConfig();

    public List<List<Object>> assertWindowedOutput(final List<Tuple> inputTuples);
}
