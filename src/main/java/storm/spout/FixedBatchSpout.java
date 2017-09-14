package storm.spout;


import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;


public class FixedBatchSpout implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;

    public FixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields; // 输出字段
        this.outputs = outputs;  // 保存至本地, 每个对象都是一个List<Object>
        this.maxBatchSize = maxBatchSize; //  该批次最大发射次数，但是不是唯一决定元素
    }

    int index = 0;
    boolean cycle = false;

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        if(index>=outputs.length && cycle) {
            index = 0;  // 超过下标后，让index归零， 继续循环发送
        }
        //  在不超过outputs大小的情况下，每次发射一个List<Object>
        for(int i=0; index < outputs.length && i < maxBatchSize; index++, i++) {
            collector.emit(outputs[index]);
        }
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1); // 最大并行度，默认是1. 好像没提供接口来修改， 很奇怪。
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields ;  // 输出字段
    }
}