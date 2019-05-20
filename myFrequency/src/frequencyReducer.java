
import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Set;
//
class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}
// myWordcount Reducer class
public class frequencyReducer extends 
Reducer<Text, MapWritable, Text, MapWritable> {
 
	 private MyMapWritable map = new MyMapWritable();

	    @Override
	    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	       map.clear();
	       for (MapWritable value : values) {
	           addAll(value);
	       }
	       //f(B|A)를 구하는 과정
	       int sum =0;
	       //map에 있는 모든 value들를 더하여 sum에 저장한다.
	       for (Object k: map.keySet()) {
	    	   DoubleWritable i = (DoubleWritable)map.get(k);
	    	   sum+= i.get();
	       }
	       //모든 value들을 sum으로 나누어 준다.
	       for (Object k: map.keySet()) {
	    	   DoubleWritable i = (DoubleWritable)map.get(k);
	    	   i.set(i.get()/sum);
	    	   map.put((Writable) k, i);
	       	}
	       context.write(key,map);
	    }
	    private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            DoubleWritable fromCount = (DoubleWritable) mapWritable.get(key);
	            if (map.containsKey(key)) {
	                DoubleWritable count = (DoubleWritable) map.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                map.put(key, fromCount);
	            }
	        }
	    }
}