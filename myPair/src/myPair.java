
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
public class myPair {
	
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        // making job object
        Job job = new Job(conf, "pair");
        // setting .jar class that has main function
        job.setJarByClass(myPair.class);
        
        // setting mapper, combiner, reducer class
        job.setMapperClass(pairMapper.class);
        job.setCombinerClass(pairReducer.class);
        job.setReducerClass(pairReducer.class);
        
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
