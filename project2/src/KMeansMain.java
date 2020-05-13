import cluster.KMeans;
import cluster.PrintCluster;
import utils.FileOperator;

import java.io.IOException;
import java.util.Random;

public class KMeansMain {
	public static String globalHdfsPath = "hdfs://127.0.1.1:9000";
	public static String meansPath = "/project2/result/k-means.txt";
	public static String tmpMeansPath = "/project2/result/k-means_tmp";
	public static String srcDataPath = "/project2/data/cluster.txt";
	public static String printClusterPath = "/project2/result/cluster_result";
	public static int k = 20, epoch = 20;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		prepareMeansFile();
		for (int i = 0; i < epoch; i++) {
			System.out.println("running " + i + "th epoch.");
			try {
				KMeans.run(globalHdfsPath, meansPath, srcDataPath, tmpMeansPath);
				try {
					FileOperator.rm(globalHdfsPath, meansPath);
					if (FileOperator.rename(globalHdfsPath, tmpMeansPath + "/part-r-00000", meansPath)) {
						FileOperator.rmdir(globalHdfsPath, tmpMeansPath);
					} else {
						System.out.println("rename file has error.");
						return;
					}
				} catch (Exception e) {
					System.out.println("rename file has error.");
					return;
				}
			} catch (Exception e) {
				System.out.println("The " + i + "th epoch has stopped");
				return;
			}
		}
		PrintCluster.run(globalHdfsPath, meansPath, srcDataPath, printClusterPath);
	}
	// 初始化中心点、删除多余文件。
	public static void prepareMeansFile() {
		try{
			FileOperator.rmdir(globalHdfsPath, printClusterPath);
			FileOperator.rmdir(globalHdfsPath, tmpMeansPath);
		} catch (IOException e) {
		}
		try {
			FileOperator.touch(globalHdfsPath, meansPath);
		} catch (IOException e) {
			try {
				FileOperator.rm(globalHdfsPath, meansPath);
				FileOperator.touch(globalHdfsPath, meansPath);
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		Random r = new Random(System.currentTimeMillis());
		StringBuilder dataToWrite = new StringBuilder();
		for (int i = 0; i < k; i++) {
			for (int j = 0; j < 20; j++) {
				dataToWrite.append(r.nextDouble() * 10.0 - 5.0);
				if (j < 19) {
					dataToWrite.append(",");
				}
			}
			dataToWrite.append("\n");
		}
		try {
			FileOperator.appendContentToFile(globalHdfsPath, dataToWrite.toString(), meansPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}