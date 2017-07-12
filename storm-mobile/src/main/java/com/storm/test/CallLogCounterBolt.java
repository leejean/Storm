package com.storm.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class CallLogCounterBolt implements IRichBolt {
	Map<String, Map<String, Integer>> counterMap;
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Map<String, Integer>>();
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		String call = tuple.getString(0);
		Integer duration = tuple.getInteger(1);

		if (!counterMap.containsKey(call)) {
			Map<String, Integer> record = new HashMap<String, Integer>();
			record.put("count", 1);
			record.put("duration", duration);
			counterMap.put(call, record);
		} else {
			Map<String, Integer> record = counterMap.get(call);
			Integer count = record.get("count") + 1;
			Integer dur = record.get("duration") + duration;
			
			record.put("count", count);
			record.put("duration", dur);
			counterMap.put(call, record);
		}

		collector.ack(tuple);
	}

	// bolt 停止时调用此方法
	public void cleanup() {
		for(String mobile : counterMap.keySet()){
			Map<String, Integer> record = counterMap.get(mobile);
			System.out.println(mobile + " : " + record.get("count") + "," + record.get("duration"));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}

