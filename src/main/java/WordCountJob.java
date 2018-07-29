import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountJob {

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage : WordCountJob <Input Path> <Output Path>");
			System.exit(0);
		}
		Configuration conf=new Configuration();

		@SuppressWarnings("deprecation")
		Job job=new Job(conf, "wordCount");
		job.setJarByClass(WordCountJob.class);
		
		job.setJobName("Word Count");
		
		job.setMapperClass(WordCountMapper.class);
		//The reduce phase can be optimized by combining the output of the map phase at the map node itself. 
		//This is an optimization of the reduce phase to allow it to work on data that has been "partially reduced".
		job.setCombinerClass(WordCountReduce.class);
		job.setReducerClass(WordCountReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
