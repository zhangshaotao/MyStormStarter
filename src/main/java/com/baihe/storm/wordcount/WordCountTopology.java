package com.baihe.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		TopologyBuilder topoBuilder = new TopologyBuilder();
		
		topoBuilder.setSpout("random-words-spout",new RandomWordsSpout(),3);
		topoBuilder.setBolt("sentence-split-bolt", new SentenceSplitBolt(),3).shuffleGrouping("random-words-spout");
		topoBuilder.setBolt("word-counter-bolt", new WordCountBolt()).fieldsGrouping("sentence-split-bolt", new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(false);
		
	    if (args != null && args.length > 0)
	    {
	        conf.setNumWorkers(3);

	        StormSubmitter.submitTopology(args[0], conf, topoBuilder.createTopology());
	    }
	    else
	    {
	        conf.setMaxTaskParallelism(3);

	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("word-count", conf, topoBuilder.createTopology());

	        Thread.sleep(10000);

	        cluster.shutdown();
	      }
	}

}
