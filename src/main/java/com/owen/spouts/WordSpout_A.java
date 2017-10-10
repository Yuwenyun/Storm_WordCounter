package com.owen.spouts;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordSpout_A extends WordSpout<String, String>
{
    public Logger logger = LoggerFactory.getLogger(WordSpout_A.class);

    public WordSpout_A(String name, String streamId, String[] fields)
    {
        super(name, streamId, fields);
    }

    public void nextTuple()
    {
        ConsumerRecords<String, String> records = this.consumer.poll(100);
        if(records != null)
        {
            logger.info("Processing new records: " + records.toString());
            for(ConsumerRecord<String, String> record : records)
            {
                String tuple = (String)record.value();
                logger.info("Emitting tuple: " + tuple);
                this.collector.emit(this.streamId, new Values(tuple));
            }
        }
    }
}
