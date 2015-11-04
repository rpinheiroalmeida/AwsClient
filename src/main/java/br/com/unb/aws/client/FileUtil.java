package br.com.unb.aws.client;

import java.io.File;
import java.io.FileFilter;

public class FileUtil {

	public static String getFileName(String filePath) {
		return filePath.substring(filePath.lastIndexOf("/")+1, filePath.length());
	}
	
	public static boolean isDirectory(String filePath) {
		File file = new File(filePath);
		return file.isDirectory();
	}
	
	public static File[] getFiles(String directory) {
		File file = new File(directory);
		
		FileFilter filter = new FileFilter() {
            public boolean accept(File pathname) {
               return pathname.isFile();
            }
         };
         
		return file.listFiles(filter);
	}
}
