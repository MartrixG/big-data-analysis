package operator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Sample {
	public static boolean run(String globalHdfsPath, String InputPath, String OutputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		Job job = Job.getInstance(conf, "Sample");
		job.setJarByClass(Sample.class);
		// Map设置
		job.setMapperClass(SampleMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(SampleReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 输出文件设置
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(InputPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(OutputPath));

		return job.waitForCompletion(true);
	}

	static class SampleMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 只对职业进行信息提取, 传递给Reducer
			Text keyToWrite = new Text(value.toString().split("\\|")[10].toLowerCase());
			context.write(keyToWrite, value);
		}
	}

	static class SampleReduce extends Reducer<Text, Text, NullWritable, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value : values) {
				// 随机取值, 保证采样率为0.2
				if(Math.random() <= 0.2)
					context.write(NullWritable.get(), new Text(value.toString()));
			}
		}
	}
}

