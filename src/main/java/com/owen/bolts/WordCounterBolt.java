package com.owen.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class WordCounterBolt implements IRichBolt
{
    public Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
    private String name;
    protected String streamId;
    protected String[] fields;

    protected OutputCollector collector;
    protected Map<String, String> counter;

    public WordCounterBolt(String name, String streamId, String[] fields)
    {
        this.name = name;
        this.streamId = streamId;
        this.fields = fields;
        this.counter = new HashMap<String, String>();
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    public void cleanup()
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        if(this.streamId != null)
            declarer.declareStream(this.streamId, new Fields(this.fields));
        else if(this.fields != null)
            declarer.declare(new Fields(this.fields));
    }

    public Map<String, Object> getComponentConfiguration() { return null; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
