
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
	//MapWritable 선언
	private MapWritable map = new MapWritable();
    private Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        if (words.length > 1) {
            for (int i = 0; i < words.length; i++) {
                word.set(words[i]);
                map.clear();

                for (int j = 0; j < words.length; j++) {
                    if (j == i) continue;
                    Text neighbor = new Text(words[j]);
                    if(map.containsKey(neighbor)){
                    	//double형 변수를 담아야 하기 때문에 DoubleWritable로 count를 선언
                    	DoubleWritable count = (DoubleWritable)map.get(neighbor);
                    	count.set(count.get()+1);
                    	map.put(neighbor, count);
                    }else{
                        map.put(neighbor,new DoubleWritable(1));
                    }
                }
              context.write(word,map);
            }
        }
    }
}


