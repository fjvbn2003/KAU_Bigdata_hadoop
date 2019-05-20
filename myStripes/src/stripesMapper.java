
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

//pair Mapper class
public class stripesMapper extends
Mapper<Object, Text, Text, MapWritable> {
	//각각의 단어마다 다른 단어의 발생빈도를 담든 하둡의 map자료형 선언
	private MapWritable map = new MapWritable();
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //  " "(공백)을 기준으로 단어들을 split해서 words배열에 담는다.
    	String[] words = value.toString().split(" ");
        if (words.length > 1) {
            for (int i = 0; i < words.length; i++) {
            	//각각의 단어마다 연관 단어의 빈도수를 담는 map을 선언한다.
                word.set(words[i]);
                map.clear();
                //현재 words에 담겨있는 단어를 모두 순회하면서 map을 업데이트 한다.
                for (int j = 0; j < words.length; j++) {
                	// 자기 자신일 경우 건너 뜀
                    if (j == i) continue;  
                    Text neighbor = new Text(words[j]);
                    //만약 neighbor에 해당하는 단어가 map에 있다면,
                    if(map.containsKey(neighbor)){
                    	//해당 단어의 count value를 1만큼 증가
                    	IntWritable count = (IntWritable)map.get(neighbor);
                    	count.set(count.get()+1);
                    	map.put(neighbor, count);
                    	//아니라면, map에 <neighbor,1> pair를 생성
                    }else{
                        map.put(neighbor,new IntWritable(1));
                    }
                }
              context.write(word,map);
            }
        }
    }
}


