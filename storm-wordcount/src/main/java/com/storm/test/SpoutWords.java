package com.storm.test;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 数据源头
 * @author Leejean
 *
 */
public class SpoutWords extends BaseRichSpout{

    private SpoutOutputCollector spoutOutputCollector;

    //为了简单,定义一个静态数据模拟不断的数据流产生
    private static final String[] sentences={
            "The core abstraction in Storm is the stream",
            "The basic primitives Storm provides for doing stream transformations ",
            "Spouts and bolts have interfaces that you implement to run your application-specific logic"
    };

    private int index=0;

    /**
     * 初始化操作
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 核心逻辑
     */
    public void nextTuple() {
        // 发射数据，Values 需要与 Fields 对应
        spoutOutputCollector.emit(new Values(sentences[index]));

        // 控制循环读取数组数据
        ++index;
        if(index>=sentences.length){
            index=0;
        }
    }

    /**
     * 定义向下游输出数据的字段
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 定义发射数据都有哪些字段，下游 bolt 会根据字段名获取数据
        outputFieldsDeclarer.declare(new Fields("sentences"));
    }
}