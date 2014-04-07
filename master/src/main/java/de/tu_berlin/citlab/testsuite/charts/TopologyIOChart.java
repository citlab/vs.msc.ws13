package de.tu_berlin.citlab.testsuite.charts;


import backtype.storm.tuple.Tuple;
import com.xeiam.xchart.BitmapEncoder;
import com.xeiam.xchart.Chart;
import com.xeiam.xchart.ChartBuilder;
import com.xeiam.xchart.StyleManager;

import java.io.IOException;
import java.util.*;

/**
 * Created by Constantin on 06.04.2014.
 */
public class TopologyIOChart
{
    private final static int chartWidth = 1200;
    private final static int chartHeight = 600;


    private final String testName;
    private final List<String> xBoltNames;
    private final List<Long> yInputTuples;


    private double count = 0;


    public TopologyIOChart(String testName)
    {
        this.testName = testName;

        this.xBoltNames = new ArrayList<>();
        this.yInputTuples= new ArrayList<>();
    }


    public void addBoltInputToChart(long inputTupleCount, String boltName)
    {
        if(boltName.contains(testName+"/")){
            String[] nameSplit = boltName.split(testName+"/");
            xBoltNames.add(nameSplit[1]);
        }
        else{
            xBoltNames.add(boltName);
        }
//        xBoltNames.add(count);
        yInputTuples.add(inputTupleCount);
        count ++;
    }


    public void createChart() {
        String chartName = "Topology-Chain for " + testName;
        String xAxisName = "Bolt-Names";
        String yAxisName = "InputTuple Count";
        Chart chart = new ChartBuilder().chartType(StyleManager.ChartType.Bar).width(chartWidth).height(chartHeight)
                .title(chartName).xAxisTitle(xAxisName).yAxisTitle(yAxisName).build();


        // Customize Chart
        chart.getStyleManager().setLegendPosition(StyleManager.LegendPosition.OutsideE);
        chart.getStyleManager().setXAxisMin(0L);
        chart.getStyleManager().setXAxisMax(xBoltNames.size());
        chart.getStyleManager().setYAxisMin(0L);
        chart.getStyleManager().setBarWidthPercentage(0.25);
        chart.getStyleManager().setAxisTitlePadding(20);

//            chart.getStyleManager().setYAxisTickMarkSpacingHint(Math.round(chartHeight / yInputTuples.size()));
//        chart.getStyleManager().setXAxisTickMarkSpacingHint(Math.round(chartWidth / xBoltNames.size()));

        chart.addSeries("Input Tuples per Bolt", xBoltNames, yInputTuples);
//            chart.addSeries("Output Values per Bolt", xBoltNames, yOutputValues);

        try {
            BitmapEncoder.savePNG(chart, "logs/" + testName + "/TopologyChain_" + testName + ".png");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}