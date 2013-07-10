package practice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SecondarySort {
	public static class IntPair implements WritableComparable<IntPair> {
		int first, second;

		public int getFirst() {
			return first;
		}

		public int getSecond() {
			return second;
		}

		public void set(int left, int right) {
			first = left;
			second = right;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			first = input.readInt();
			second = input.readInt();
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeInt(first);
			output.writeInt(second);
		}

		@Override
		public int compareTo(IntPair o) {
			if (first != o.first) {
				return first < o.first ? -1 : 1;
			} else if (second != o.second) {
				return second < o.second ? -1 : 1;
			}
			return 0;
		}

		public int hashCode() {
			return first * 157 + second;
		}

		public boolean equals(Object right) {
			if (right == null) {
				return false;
			}
			if (this == right) {
				return true;
			}
			if (right instanceof IntPair) {
				IntPair r = (IntPair) right;
				return r.first == first && r.second == second;
			}
			return false;
		}
	}

	public static class FirstPartitioner extends
			Partitioner<IntPair, IntWritable> {

		@Override
		public int getPartition(IntPair key, IntWritable value,
				int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}

	}

	public static class GroupingComparator extends WritableComparator {

		protected GroupingComparator() {
			super(IntPair.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int l = ip1.getFirst();
			int r = ip2.getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntPair, IntWritable> {
		private final IntPair intkey = new IntPair();
		private final IntWritable intValue = new IntWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			if (tokenizer.hasMoreTokens()) {
				int left = Integer.parseInt(tokenizer.nextToken()), right;
				if (tokenizer.hasMoreTokens()) {
					right = Integer.parseInt(tokenizer.nextToken());
					intkey.set(left, right);
					intValue.set(right);
					context.write(intkey, intValue);
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<IntPair, IntWritable, Text, IntWritable> {
		private final Text left = new Text();
		private static final Text SEPARATOR = new Text(
				"------------------------------------------------");

		public void reduce(IntPair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(SEPARATOR, null);
			left.set(Integer.toString(key.getFirst()));
			for (IntWritable val : values) {
				context.write(left, val);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "secondarySort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setMapOutputKeyClass(IntPair.class);
		// map输出Value的类型
		job.setMapOutputValueClass(IntWritable.class);
		// rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(Text.class);
		// rduce输出Value的类型
		job.setOutputValueClass(IntWritable.class);

		// 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
		job.setInputFormatClass(TextInputFormat.class);
		// 提供一个RecordWriter的实现，负责数据输出。
		job.setOutputFormatClass(TextOutputFormat.class);

		// 输入hdfs路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 输出hdfs路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 提交job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
