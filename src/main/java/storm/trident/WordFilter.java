package storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-1
 * Time: 上午9:03
 * To change this template use File | Settings | File Templates.
 */
public class WordFilter extends BaseFilter {

    String actor;
    public WordFilter(String actor) {
        this.actor = actor;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        if(tuple.getString(0).equals(actor)){
//            System.out.println("Filter word is:"+tuple.getString(0) + "  and return type is:"+tuple.getString(0).equals(actor));
        }
        return tuple.getString(0).equals(actor);
    }
}
