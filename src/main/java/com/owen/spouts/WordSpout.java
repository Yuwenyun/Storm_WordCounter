package com.owen.spouts;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class WordSpout<K, V> implements IRichSpout
{
    public static Logger logger = LoggerFactory.getLogger(WordSpout.class);
    protected SpoutOutputCollector collector;
    protected KafkaConsumer<K, V> consumer;
    private Properties props;
    private List<String> topics;

    private String name;
    protected String streamId;
    protected String[] fields;

    public WordSpout(String name, String streamId, String[] fields)
    {
        this.name = name;
        this.streamId = streamId;
        this.fields = fields;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    public void close()
    {
        if(this.consumer != null)
            this.consumer.close();
    }

    public void activate()
    {
        logger.info("Activating the consumer of " + this.name);
        if(this.consumer == null)
        {
            if(this.props != null)
            {
                logger.info("Initializing consumer with props: " + this.props.toString());
                this.consumer = new KafkaConsumer<K, V>(this.props);
                if(this.topics != null)
                {
                    this.consumer.subscribe(this.topics);
                    logger.info("Consumer of " + this.name + " consuming [" + this.topics.toString() + "]");
                }
                else
                    logger.warn("No topics for consumer to listen.");
            }
            else
                logger.warn("No properties specified for consumer.");
        }
        else
        {
            try
            {
                this.consumer.wakeup();
            }
            catch (Exception e)
            {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void deactivate()
    {
        logger.info("Deactivating the consumer of " + this.name);
        this.consumer.paused();
    }

    public void ack(Object msgId) { }
    public void fail(Object msgId) { }
    public Properties getProps() { return props; }
    public void setProps(Properties props) { this.props = props; }
    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }
    public String getName() { return name; }
    public String getStreamId(){ return streamId; }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        if(this.streamId != null)
            declarer.declareStream(this.streamId, new Fields(this.fields));
        else if(this.fields != null)
            declarer.declare(new Fields(this.fields));
    }

    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
}
