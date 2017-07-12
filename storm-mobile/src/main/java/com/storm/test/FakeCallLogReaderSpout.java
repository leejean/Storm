package com.storm.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeCallLogReaderSpout extends BaseRichSpout {
  private SpoutOutputCollector collector;
  private boolean completed = false;

  private TopologyContext context;

  private Random randomGenerator = new Random();
  private Integer idx = 0;

  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.context = topologyContext;
    this.collector = spoutOutputCollector;
  }

  public void nextTuple() {
    if (this.idx <= 1000) {
      List<String> mobileNumbers = new ArrayList<String>();
      mobileNumbers.add("1234123401");
      mobileNumbers.add("1234123402");
      mobileNumbers.add("1234123403");
      mobileNumbers.add("1234123404");

      Integer localIdx = 0;
      while (localIdx++ < 100 && this.idx++ < 1000) {
        // 随机生成通话记录
        String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
        String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));

        while (fromMobileNumber == toMobileNumber) {
          toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
        }

        Integer duration = randomGenerator.nextInt(60);
        this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
      }
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("from", "to", "duration"));
  }

}