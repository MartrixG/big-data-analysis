package operator;

import evaluation.Evaluation;
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

public class MaxMin {
	public static boolean run(String globalHdfsPath, String InputPath, String OutputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		Job job = Job.getInstance(conf, "MaxMin");
		job.setJarByClass(MaxMin.class);
		// Map设置
		job.setMapperClass(MaxMinMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Evaluation.class);

		// Reduce设置
		job.setReducerClass(MaxMinReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// 输出文件设置
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(InputPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(OutputPath));

		return job.waitForCompletion(true);
	}

	static class MaxMinMap extends Mapper<LongWritable, Text, Text, Evaluation> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Evaluation tmp = new Evaluation(value.toString());
			// 计算最值同样需要先过滤掉需要排除的奇异值
			if(8.1461259 <= tmp.longitude && tmp.longitude <= 11.1993265 && 56.5824856 <= tmp.latitude && tmp.latitude <= 57.750511){
				context.write(new Text("0"), tmp);//为了输出到一个Reducer中，key全部设为0
			}
		}
	}

	static class MaxMinReduce extends Reducer<Text, Evaluation, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Evaluation> values, Context context) throws IOException, InterruptedException {
			Double minRating = Double.POSITIVE_INFINITY, maxRating = Double.NEGATIVE_INFINITY, ratingSum = 0.0, tot = 0.0;
			for(Evaluation value : values){
				// 过滤掉离群值和空值（-1）
				if(value.rating <= 100.0 && value.rating >= 0.0) {
					minRating = Double.min(minRating, value.rating);
					maxRating = Double.max(maxRating, value.rating);
					tot++;
					ratingSum += value.rating;
				}
			}
			ratingSum /= tot;
			// 将最大值、最小值、平均值按照顺序写入文件
			context.write(NullWritable.get(), new Text(maxRating.toString()));
			context.write(NullWritable.get(), new Text(minRating.toString()));
			context.write(NullWritable.get(), new Text(ratingSum.toString()));
		}
	}
}
