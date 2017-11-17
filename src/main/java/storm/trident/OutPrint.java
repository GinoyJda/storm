package storm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-1
 * Time: 下午1:51
 * To change this template use File | Settings | File Templates.
 */
public class OutPrint  extends BaseFunction {
    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        String str = objects.getString(0);
//        System.out.println("the str is:"+ ((Tuple)tridentCollector).getString(0) );
        tridentCollector.emit(new Values(str,getRandom()));
    }

    private int getRandom() {
        int max=4;
        int min=0;
        Random random = new Random();
        return  random.nextInt(max)%(max-min+1) + min;
    }
}
