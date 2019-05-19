
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

//pair Mapper class
public class frequencyMapper extends
Mapper<Object, Text, Text, MapWritable> {
		
	private MapWritable occurrenceMap = new MapWritable();
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                word.set(tokens[i]);
                occurrenceMap.clear();

                for (int j = 0; j < tokens.length; j++) {
                    if (j == i) continue;
                    Text neighbor = new Text(tokens[j]);
                    if(occurrenceMap.containsKey(neighbor)){
                       DoubleWritable count = (DoubleWritable)occurrenceMap.get(neighbor);
                       count.set(count.get()+1);
                       occurrenceMap.put(neighbor, count);
                    }else{
                        occurrenceMap.put(neighbor,new DoubleWritable(1));
                    }
                }
              context.write(word,occurrenceMap);
            }
        }
    }
}


