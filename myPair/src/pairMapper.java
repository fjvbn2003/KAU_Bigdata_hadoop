
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//pair Mapper class
public class pairMapper extends
Mapper<Object, Text, Text, IntWritable> {
		
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        protected void map(Object key, Text value,Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
            String[] words = value.toString().split(" ");
            
            for(int i=0; i<words.length; i++) {
            	for(int j=0; j<words.length; j++) {
                    word.set(words[i]+' '+words[j]);
                    context.write(word, one);
            	}
            }
            
        }
}


