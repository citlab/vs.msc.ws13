package de.tu_berlin.citlab.testsuite.diagrams;


import backtype.storm.tuple.Tuple;
import com.xeiam.xchart.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by Constantin on 06.04.2014.
 */
public class BoltTupleChart
{
    private final static int chartWidth = 1200;
    private final static int chartHeight = 600;


    private final String boltName;
    private final Set<Tuple> currentTuples;
    private final List<Long> timeStampArr;
    private final Map<Long, Set<Tuple>> allTuples;

    private final List<Collection<Long>> xDataList;
    private final List<Collection<Long>> yDataList;

    public BoltTupleChart(String boltName)
    {
        this.boltName = boltName;

        this.xDataList = new ArrayList<>();
        this.yDataList = new ArrayList<>();
        this.currentTuples = new HashSet<>();
        this.allTuples = new HashMap<>();
        this.timeStampArr = new ArrayList<>();
    }


    public void addTupleToChart(Tuple tuple, long timeStamp)
    {
        currentTuples.add(tuple);
        Set<Tuple> tupleCopy = new HashSet<>(currentTuples);
        allTuples.put(timeStamp, tupleCopy);
        timeStampArr.add(timeStamp);
    }

    public void flushWindow()
    {
        currentTuples.clear();
    }


    public void createChart(Long startTime, Long endTime)
    {
        long maxYVal = prepareChart(startTime);
        if(maxYVal > 0){
        // Create Chart
            String chartName = "Arrived Tuples on Bolt "+ boltName;
            String xAxisName = "TimeDiff [ms]";
            String yAxisName = "Input-Tuples";
            Chart chart = new ChartBuilder().chartType(StyleManager.ChartType.Scatter).width(chartWidth).height(chartHeight)
                                            .title(chartName).xAxisTitle(xAxisName).yAxisTitle(yAxisName).build();
//            chart.getStyleManager().setChartType(StyleManager.ChartType.Scatter);

        // Customize Chart
            chart.getStyleManager().setLegendPosition(StyleManager.LegendPosition.OutsideE);
            chart.getStyleManager().setXAxisMin(0L);
            chart.getStyleManager().setXAxisMax(endTime - startTime);
            chart.getStyleManager().setYAxisMin(1L);

            chart.getStyleManager().setYAxisTickMarkSpacingHint(Math.round(chartHeight / maxYVal));
            chart.getStyleManager().setBarWidthPercentage(.01);

        // Add Series
            for (int i = 0; i < xDataList.size(); i++) {
                String seriesLegend = "Tuples waiting for "+
                                      "executeBatches() Iteration "+ i;
                chart.addSeries(seriesLegend, xDataList.get(i), yDataList.get(i));
            }

            try {
                BitmapEncoder.savePNG(chart, "./Tuples_"+boltName+".png");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }



    private long prepareChart(Long startTime)
    {
        int currSeriesIndex = 0;
        long maxYSize = 0;

        Long[] sortedTimeStamps = new Long[timeStampArr.size()];
        sortedTimeStamps= timeStampArr.toArray(sortedTimeStamps);
        Arrays.sort(sortedTimeStamps);
        for (int n = 0; n < sortedTimeStamps.length; n++) {
            Long actTimeStamp = sortedTimeStamps[n];
            Set<Tuple> actTupleSet = allTuples.get(actTimeStamp);

            long i;
            for (i = 1; i <= actTupleSet.size(); i++) {
                Collection<Long> seriesXData;
                Collection<Long> seriesYData;
                try {
                    seriesXData = xDataList.get(currSeriesIndex);
                    seriesYData = yDataList.get(currSeriesIndex);
                    seriesXData.add(actTimeStamp - startTime);
                    seriesYData.add(i);
                }
                catch (IndexOutOfBoundsException e){
                    seriesXData = new ArrayList<>();
                    seriesYData = new ArrayList<>();
                    seriesXData.add(actTimeStamp - startTime);
                    seriesYData.add(i);
                    xDataList.add(currSeriesIndex, seriesXData);
                    yDataList.add(currSeriesIndex, seriesYData);
                }
            }

            //Scan for the maximum Y-Values in the whole Chart:
            if(actTupleSet.size() > maxYSize){
                maxYSize = i;
            }

            if(n+1 < sortedTimeStamps.length){
                Set<Tuple> succTupleSet = allTuples.get(sortedTimeStamps[n+1]);

                //Check whether a window-flush has happend in the dataList-successor:
                if(actTupleSet.size() > succTupleSet.size()){
                    currSeriesIndex++;
                }
            }
        }
        return maxYSize;
    }
}