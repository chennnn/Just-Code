package cookie;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class CookieNumberOfDay {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s[] = value.toString().split("\t");
			context.write(new Text(s[0].trim()), new Text(s[1].trim()));
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String maxString = "";
			for (Text i : values) {
				if (i.toString().compareTo(maxString) > 0) {
					maxString = i.toString();
				}
			}
			context.write(new Text(maxString), new Text(key));
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text i : values) {
				sum++;
			}
			context.write(new Text(key), new IntWritable(sum));
		}
	}

	public static boolean run1(String args0, String args1, String description)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, description);
		job.setJarByClass(CookieNumberOfDay.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args0));
		FileOutputFormat.setOutputPath(job, new Path(args1));
		return job.waitForCompletion(true);
	}

	public static boolean run2(String args0, String args1, String description)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, description);
		job.setJarByClass(CookieNumberOfDay.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args0));
		FileOutputFormat.setOutputPath(job, new Path(args1));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
//		run2(args[0], args[2], "every day's cookie number");
		if (run1(args[0], args[1], "find the newest cookies")) {
			System.exit(run2(args[1]+"/part*", args[2], "every day's cookie number") ? 0 : 1);
		}
	}
}
