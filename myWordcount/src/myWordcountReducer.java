import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// myWordcount Reducer class
public class myWordcountReducer extends 
Reducer<Text, IntWritable, Text, IntWritable> {
 
    private IntWritable res = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int sum = 0;
        // Mapping <Key,[values]> to <Key, sum([values])>
        // iterating through values
        for(IntWritable val : values)
        {
        	//calculating sum of values
            sum += val.get();
        }
        // setting sum of values to IntWritable object
        res.set(sum);
        // emit <key, sum(values)> pair
        context.write(key, res);
	}
}

