import operator.Filter;
import operator.MaxMin;
import operator.Sample;
import utils.FileOperator;

import java.io.IOException;

public class Main {
	public static String globalHdfsPath = "hdfs://127.0.1.1:9000", localFilePath = "/opt/bd_project1/data.txt", hdfsDataPath = "/data.txt";
	public static String sampleInputPath = hdfsDataPath, sampleOutputPath = "/D_Sample";
	public static String ratingMaxMinInputPath = sampleOutputPath + "/part-r-00000", ratingMaxMinOutputPath = "/ratingMaxMin";
	public static String filterInputPath = sampleOutputPath + "/part-r-00000", filterOutputPath = "/D_Filter";
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		if(!FileOperator.testExit(globalHdfsPath, hdfsDataPath)){//检测数据是否上传hdfs
			System.out.println("未上传数据，正在上传原始数据");
			FileOperator.putFileToHDFS(globalHdfsPath, localFilePath, hdfsDataPath);
			System.out.println("原始数据已上传至HDFS");
		}

		if(!FileOperator.testExit(globalHdfsPath, sampleInputPath)){//检测Sample输入文件
			System.out.println("Sample部分输入文件不存在，请检查文件路径");
			return;
		}
		if(FileOperator.testExit(globalHdfsPath, sampleOutputPath)){
			FileOperator.rmdir(globalHdfsPath, sampleOutputPath);
			System.out.println("Sample部分输出文件已存在，已删除该文件");
		}
		if(Sample.run(globalHdfsPath, sampleInputPath, sampleOutputPath)){
			System.out.println("Sample任务成功完成");
		}

		if(!FileOperator.testExit(globalHdfsPath, ratingMaxMinInputPath)){//检测Rating输入文件
			System.out.println("Rating最值计算部分输入文件不存在，请检查文件路径");
			return;
		}
		if(FileOperator.testExit(globalHdfsPath, ratingMaxMinOutputPath)){
			FileOperator.rmdir(globalHdfsPath, ratingMaxMinOutputPath);
			System.out.println("Rating部分输出文件已存在，已删除该文件");
		}
		if(MaxMin.run(globalHdfsPath, ratingMaxMinInputPath, ratingMaxMinOutputPath)){
			System.out.println("已计算出rating最值");
		}


		if(!FileOperator.testExit(globalHdfsPath, filterInputPath)){//检测Filter输入文件
			System.out.println("Filter部分输入文件不存在，请检查文件路径");
			return;
		}
		if(FileOperator.testExit(globalHdfsPath, filterOutputPath)){
			FileOperator.rmdir(globalHdfsPath, filterOutputPath);
			System.out.println("Filter部分输出文件已存在，已删除该文件");
		}
		if(Filter.run(globalHdfsPath, filterInputPath, filterOutputPath, ratingMaxMinOutputPath)){
			System.out.println("Filter任务成功完成");
		}
	}
}
