package de.tu_berlin.citlab.storm.tests.twitter.topologyTests;

import de.tu_berlin.citlab.storm.tests.twitter.helpers.BadWord;
import de.tu_berlin.citlab.storm.tests.twitter.helpers.STORAGE;
import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.XMLConfigurationFactory;
import org.junit.BeforeClass;

import java.util.List;


public class TestingEnvironment extends TopologyTest
{
    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.BOLTTEST_ID);


    @BeforeClass
    public static void initEnvironment()
    {
        STORAGE.badWords.put("bombe", new BadWord("bombe", 100));
        STORAGE.badWords.put("nuklear", new BadWord("nuklear", 1000));
        STORAGE.badWords.put("anschlag", new BadWord("anschlag", 200));
        STORAGE.badWords.put("religion", new BadWord("religion", 100));
        STORAGE.badWords.put("macht", new BadWord("macht", 300));
        STORAGE.badWords.put("kampf", new BadWord("kampf", 300));
    }

    @Override
    public String nameTopologyTest() {
        return this.getClass().getCanonicalName();
    }

    @Override
    public BoltEmission defineFirstBoltsInput() {
        return null;
    }

    @Override
    public List<BoltTestConfig> defineTopologySetup() {
        return null;
    }
}
