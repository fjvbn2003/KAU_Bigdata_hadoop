
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//myWordcount Driver class
public class myWordcount {
	
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        // making job object
        Job job = new Job(conf, "WordCount");
        // setting .jar class that has main function
        job.setJarByClass(myWordcount.class);
        
        // setting combiner class

       
        // setting mapper, reduceer class
        job.setMapperClass(myWordcountMapper.class);
        job.setCombinerClass(myWordcountReducer.class);
        job.setReducerClass(myWordcountReducer.class);
        
        // setting input, output format class
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // setting output-Key, output-Value  class
        // key is Text, value is Integer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // specifying input/output Hadoop file-system path.
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // waiting for completion
        job.waitForCompletion(true);
    }
 
}