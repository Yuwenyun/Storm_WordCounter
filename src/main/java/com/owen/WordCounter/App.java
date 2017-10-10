package com.owen.WordCounter;

import com.owen.storm.WordCounterTopologyBuilder;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Config config = new Config();
        config.setNumWorkers(1);
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-counter-topology", config, WordCounterTopologyBuilder.build().createTopology());
    }
}
