
package storm.es;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Basic bolt for storing tuple to ES document.
 */
public class NewEsIndexBolt extends NewAbstractEsBolt {
    private final EsTupleMapper tupleMapper;

    /**
     * EsIndexBolt constructor
     * @param esConfig Elasticsearch configuration containing node addresses and cluster name {@link EsConfig}
     * @param tupleMapper Tuple to ES document mapper {@link EsTupleMapper}
     */
    public NewEsIndexBolt(NewEsConfig esConfig, EsTupleMapper tupleMapper) {
        super(esConfig);
        this.tupleMapper = tupleMapper;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }

    /**
     * {@inheritDoc}
     * Tuple should have relevant fields (source, index, type, id) for tupleMapper to extract ES document.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            String source = tupleMapper.getSource(tuple);
            String index = tupleMapper.getIndex(tuple);
            String type = tupleMapper.getType(tuple);
            String id = tupleMapper.getId(tuple);

            client.prepareIndex(index, type, id).setSource(source).execute().actionGet();
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
