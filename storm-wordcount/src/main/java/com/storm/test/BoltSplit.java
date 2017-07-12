package com.storm.test;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BoltSplit extends BaseRichBolt{

    private OutputCollector outputCollector;

    /**
     * 初始化
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    /**
     * 每次收到数据都会执行此方法
     */
    public void execute(Tuple tuple) {
        // 根据上游设置的字段名读取数据
        String sentence = tuple.getStringByField("sentences");

        // 分词
        String[] words = sentence.split(" ");
        for(String word : words){
            // 把每个单词发射出去
            outputCollector.emit(new Values(word));
        }
    }
    
    /**
     * 定义向下游输出数据的字段
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}