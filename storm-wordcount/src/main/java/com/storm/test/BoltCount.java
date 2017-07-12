package com.storm.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BoltCount extends BaseRichBolt{

    //保存单词计数
    private Map<String,Long> wordCount = null;

    private OutputCollector outputCollector;

    /**
     *  初始化
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        wordCount = new HashMap<String, Long>();
    }

    /**
     *  每次收到数据都会执行此方法
     */
    public void execute(Tuple tuple) {
        // 获取上游数据
        String word = tuple.getStringByField("word");

        // 计数
        Long count = wordCount.get(word);
        if(count == null){
            count = 0L;
        }
        ++count;
        wordCount.put(word,count);

        // 向下游发射此单词目前的统计结果
        outputCollector.emit(new Values(word,count));
    }

    /**
     * 定义向下游输出数据的字段
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}