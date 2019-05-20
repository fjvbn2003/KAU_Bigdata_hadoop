
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
            //읽어온 단어 배열 전체를 두 번 반복하며, 단어들의 모든 쌍을 emit 한다.
            for(int i=0; i<words.length; i++) {
            	for(int j=0; j<words.length; j++) {
            		if(i==j) {continue;}
            		//단어 두개를 space와 함께 합친다.
                    word.set(words[i]+' '+words[j]);
                    context.write(word, one);
            	}
            }
            
        }
}


