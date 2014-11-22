package com.baihe.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt{

	private Map<String,Integer> counters = new HashMap<String,Integer>();
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		
		int counter = 1;
		if(counters.containsKey(word))
		{
			counter = counters.get(word);
			counter++;
		}
		counters.put(word, counter);
		System.out.println("------word:"+word+"-------count:"+counter+"----------");
		collector.emit(new Values(word,counter));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","counter"));
		
	}
	
	

}
