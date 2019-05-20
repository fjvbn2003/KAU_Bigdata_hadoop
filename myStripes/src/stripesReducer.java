
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Set;
//출력을 위해 MatWritable의 toString을 재 정의
class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
    	// 빠른 문자열 연산을 위한 스트링 빌더 객체 선언
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();
        // 각각의 key마다, 아래와 같은 포맷으로 스트링 뒤에 추가
        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}
// myWordcount Reducer class
public class stripesReducer extends 
Reducer<Text, MapWritable, Text, MapWritable> {
	// 위에서 재정의한 MyMapWritable 객체 선언 
	private MyMapWritable map = new MyMapWritable();
	 
	    @Override
	    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	        map.clear();
	        // values에는 같은 키를 갖는 단어에 대한 여러개의 map이 담겨있다.
	        for (MapWritable value : values) {
	        	// 순회하면서 각각의 map의 value들을 합쳐나간다.
	            addAll(value);
	        }
	       context.write(key,map);
	    }
	    // map들을 합치치기 위한 함수
	    private void addAll(MapWritable mapWritable) {
	    	//mapWritable의 키들을 모두 가져온다.
	        Set<Writable> keys = mapWritable.keySet();
	        //모든 키를 순회하면서 
	        for (Writable key : keys) {
	        	//각각의 키에 해당하는 count(value)를 가져온다.
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            // 만약 기존의 map도 키를 가지고 있다면, 기존 value에 fromCount값을 더한다.
	            if (map.containsKey(key)) {
	                IntWritable count = (IntWritable) map.get(key);
	                count.set(count.get() + fromCount.get());
	            // 그렇지 않다면, 새로운 키를 생성하여 map에 더한다.
	            } else {
	                map.put(key, fromCount);
	            }
	        }
	    }
}

