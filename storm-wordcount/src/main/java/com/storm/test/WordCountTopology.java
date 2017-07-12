package com.storm.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) {

        //创建 spout 和 blot 的实例
        SpoutWords sentenceSpout = new SpoutWords();
        BoltSplit splitSentenceBolt = new BoltSplit();
        BoltCount wordCountBolt = new BoltCount();
        BoltReport reportBolt = new BoltReport();

        //拓扑Builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //配置 spout
        topologyBuilder.setSpout("spout", sentenceSpout, 2);

        //配置 split bolt,上游为 spout
        topologyBuilder.setBolt("bolt-split", splitSentenceBolt).shuffleGrouping("spout");

        //配置 count bolt,上游为 bolt-split
        topologyBuilder.setBolt("bolt-count", wordCountBolt).fieldsGrouping("bolt-split", new Fields("word"));

        //配置 report bolt,上游为 bolt-count
        topologyBuilder.setBolt("bolt-report", reportBolt).globalGrouping("bolt-count");

        Config config = new Config();

        //建立本地集群,利用LocalCluster,storm在程序启动时会在本地自动建立一个集群,不需要用户自己再搭建,方便本地开发和debug
        LocalCluster cluster = new LocalCluster();

        //创建拓扑实例,并提交到本地集群进行运行
        cluster.submitTopology("word-count", config, topologyBuilder.createTopology());
    }
}