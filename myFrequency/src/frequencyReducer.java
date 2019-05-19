
import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Set;

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
 
	 private MyMapWritable incrementingMap = new MyMapWritable();

	    @Override
	    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	        }
	       ///////////////// This is for frequency of word f(B|A)/////////////////////////
	        int sum =0;
	        //summation of values
	        for (Object k: incrementingMap.keySet()) {
	        	DoubleWritable i = (DoubleWritable)incrementingMap.get(k);
	        	sum+= i.get();
	        }
	        //update
	        for (Object k: incrementingMap.keySet()) {
	        	DoubleWritable i = (DoubleWritable)incrementingMap.get(k);
	        	i.set(i.get()/sum);
	        	incrementingMap.put((Writable) k, i);
	        }
		    ///////////////// End ///////////////

	        
	       context.write(key,incrementingMap);
	    }


	    private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            DoubleWritable fromCount = (DoubleWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                DoubleWritable count = (DoubleWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
}