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

public class KMeans {
	public static boolean run(String globalHdfsPath, String meansPath, String srcDataPath, String tmpMeansPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.set("KMeans", meansPath);
		Job job = Job.getInstance(conf, "K-means");
		job.setJarByClass(KMeans.class);

		// Map设置
		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(srcDataPath));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(tmpMeansPath));

		return job.waitForCompletion(true);
	}

	static class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		ArrayList<String> points = new ArrayList<>();
		int k;

		//启动map之前，加载k个中心值的数据
		protected void setup(Context context) throws IOException {
			String pointsData = FileOperator.getContentFromHDFS(context.getConfiguration().get("fs.defaultFS"),
					context.getConfiguration().get("KMeans"));
			String[] eachPointData = pointsData.split("\n");
			points.addAll(Arrays.asList(eachPointData));
			k = points.size();
			//System.out.println("setup--------------- "+k+" means");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int nearestPointNo = 0;
			double nearestDis = Double.MAX_VALUE;
			for (int i = 0; i < k; i++) {
				Double tmpDis = Util.getDistance(value.toString(), points.get(i));//计算和第i个中心值之间的距离
				if (tmpDis < nearestDis) { // 找到更近的中心值更新
					nearestDis = tmpDis;
					nearestPointNo = i;
				}
			}
			context.write(new LongWritable(nearestPointNo), value); // 将所有处于同一等价类的数据输出到一个同一个reducer处理
		}
	}

	static class KMeansReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//System.out.println("reduce -------------- " + key + "kind");
			double[] center = new double[20];//初始化新的中心值
			for (int i = 0; i < 20; i++) {
				center[i] = 0.0;
			}
			double count = 0;
			for (Text value : values) {
				String[] point = value.toString().split(",");
				for (int i = 0; i < 20; i++) {
					center[i] += Double.parseDouble(point[i]);// 更新中心值总和
				}
				count++;
			}
			for (int i = 0; i < 20; i++) {
				center[i] /= count;//计算平均值获得新的k个中心值
			}
			StringBuilder re = new StringBuilder();
			for (Double eachData : center) {
				re.append(eachData.toString());
				re.append(",");
			}
			re.deleteCharAt(re.length() - 1);
			context.write(NullWritable.get(), new Text(re.toString()));//将新的中心值写入文件
		}
	}
}
