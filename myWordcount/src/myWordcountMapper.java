import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//myWordCount Mapper class
public class myWordcountMapper extends
Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        protected void map(Object key, Text value,Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while(itr.hasMoreTokens())
            {
            	//bring next word from iterator
                word.set(itr.nextToken());
                //emit (Key, Value) pair 
                //Key is a word and Value is 1(IntWritable)
                context.write(word, one);
            }
        }
}


