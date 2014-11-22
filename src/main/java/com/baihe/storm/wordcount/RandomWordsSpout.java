package com.baihe.storm.wordcount;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomWordsSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	private Random random;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
		
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}

	public void nextTuple() {
		Utils.sleep(100);
		
		String[] sentences = new String[]{ 
				"the cow jumped over the moon", 
				"an apple a day keeps the doctor away",
		        "four score and seven years ago", 
		        "snow white and the seven dwarfs", 
		        "i am at two with nature" };
		String sentence = sentences[random.nextInt(sentences.length - 1)];
		
		collector.emit(new Values(sentence));
	}

}
