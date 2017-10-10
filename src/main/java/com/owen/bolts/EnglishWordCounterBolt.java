package com.owen.bolts;

import org.apache.storm.tuple.Tuple;

public class EnglishWordCounterBolt extends WordCounterBolt
{
    public EnglishWordCounterBolt(String name, String streamId, String[] fields)
    {
        super(name, streamId, fields);
    }

    public void execute(Tuple input)
    {
        String word = input.getStringByField("");

        String countString = this.counter.get(word);
        if(countString == null)
        {
            this.counter.put(word, "1");
        }
        else
        {
            int count = Integer.parseInt(countString);
            this.counter.put(word, (count + 1) + "");
        }
    }
}
