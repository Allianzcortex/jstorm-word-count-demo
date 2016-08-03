package spout;
// standard library

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;

// storm-utils
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
/**
 * Created by hzcortex on 16-7-29.
 */
public class genRandomSentenceSpout extends BaseRichSpout{
    private SpoutOutputCollector _collector;
    private Random _random;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this._collector=collector;
        this._random=new Random();
    }

    @Override
    public void nextTuple(){
        Utils.sleep(1000); //
        String[] sentences=new String[]{"this is first sentence",
        "this is second sentence","this is third sentence"};
        String sentence=sentences[this._random.nextInt(sentences.length)];
        this._collector.emit(new Values(sentence));

    }

    @Override
    public void ack(Object id){
        // 可以使用 log4j 来看
    }

    @Override
    public void fail(Object id){

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("word"));
    }


}
