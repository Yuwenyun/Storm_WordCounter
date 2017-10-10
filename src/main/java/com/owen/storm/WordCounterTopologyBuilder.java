package com.owen.storm;

import com.owen.bolts.EnglishWordCounterBolt;
import com.owen.spouts.WordSpout_A;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCounterTopologyBuilder
{
    public static TopologyBuilder build()
    {
        TopologyBuilder builder = new TopologyBuilder();
        WordSpout_A wordSpout_a = new WordSpout_A("wordSpout_a", "word_count_a", new String[]{"word"});
        EnglishWordCounterBolt englishWordCounterBolt = new EnglishWordCounterBolt("wordCounterBolt", null, null);

        builder.setSpout(wordSpout_a.getName(), wordSpout_a);
        builder.setBolt(englishWordCounterBolt.getName(), englishWordCounterBolt)
            .fieldsGrouping(wordSpout_a.getStreamId(), new Fields("word"));

        return builder;
    }
}
