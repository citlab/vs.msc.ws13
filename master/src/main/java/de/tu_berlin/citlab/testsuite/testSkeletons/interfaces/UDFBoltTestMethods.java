package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;

import java.util.List;

/**
 * Created by Constantin on 1/20/14.
 */
public interface UDFBoltTestMethods
{
    public List<Tuple> generateInputTuples();

    public Window<Tuple, List<Tuple>> initWindow();

    public WindowHandler initWindowHandler();

    public List<List<Object>> assertWindowedOutput(final List<Tuple> inputTuples);
}
