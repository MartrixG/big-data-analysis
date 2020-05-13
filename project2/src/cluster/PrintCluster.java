package cluster;

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
import utils.FileOperator;
import utils.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class PrintCluster{
	public static boolean run(String globalHdfsPath, String meansPath, String srcDataPath, String printClusterPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.set("KMeans", meansPath);
		Job job = Job.getInstance(conf, "print-cluster");
		job.setJarByClass(PrintCluster.class);

		// Map设置
		job.setMapperClass(printMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Reduce设置
		job.setReducerClass(printReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(srcDataPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(printClusterPath));

		return job.waitForCompletion(true);
	}

	static class printMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		ArrayList<String> points = new ArrayList<>();
		int k;
		// 加载中心值，方便分类
		protected void setup(Context context) throws IOException {
			String pointsData = FileOperator.getContentFromHDFS(context.getConfiguration().get("fs.defaultFS"),
					context.getConfiguration().get("KMeans"));
			String[] eachPointData = pointsData.split("\n");
			points.addAll(Arrays.asList(eachPointData));
			k = points.size();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int nearestPointNo = 0;
			double nearestDis = Double.MAX_VALUE;
			for (int i = 0; i < k; i++) {
				Double tmpDis = Util.getDistance(value.toString(), points.get(i));// 计算距离更新所距离最近的等价类
				if (tmpDis < nearestDis) {
					nearestDis = tmpDis;
					nearestPointNo = i;
				}
			}
			context.write(key, new LongWritable(nearestPointNo));//按照行号以及所属类别传递给reducer，方便输出
		}
	}

	static class printReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			for(LongWritable kind:values){
				context.write(NullWritable.get(), kind);//直接输出类别即可
			}
		}
	}
}