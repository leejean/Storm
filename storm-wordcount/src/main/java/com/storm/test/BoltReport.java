package com.storm.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class BoltReport extends BaseRichBolt {
    
    private Map<String, Long> counts = null;

    // 初始化
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counts = new HashMap<String, Long>();
    }

    // 每次收到数据都会执行此方法
    public void execute(Tuple tuple) {
        // 获取上游数据
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");

        // 保存最新计数
        counts.put(word, count);

        //打印更新后的结果
        printReport();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //无下游输出,不需要代码
    }

    // 输出整体技术信息
    private void printReport(){
        System.out.println("---------begin------");
        Set<String> words = counts.keySet();
        for(String word : words){
            System.out.println(word + " ---> " + counts.get(word));
        }
        System.out.println("---------end---------");
    }
}