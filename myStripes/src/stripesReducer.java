
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
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
public class stripesReducer extends 
Reducer<Text, MapWritable, Text, MapWritable> {
 
	 private MyMapWritable incrementingMap = new MyMapWritable();

	    @Override
	    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	        }

	       context.write(key,incrementingMap);
	    }


	    private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
}