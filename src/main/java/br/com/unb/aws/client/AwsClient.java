package br.com.unb.aws.client;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
public class AwsClient {

	private static Logger logger = Logger.getLogger(AwsClient.class);
	
	public static void main(String[] args) throws IOException {
        String bucketName = args[0];
        String jarPath = args[1];
        String mainClass = args[2];
        String inputFile1 = args[3];
        String inputFile2 = args.length == 5 ? args[4] : null;
        
        logger.info("===========================================");
        logger.info("Getting Started with Aws Client");
        logger.info("===========================================\n");

        prepareJobExecution(bucketName, jarPath, inputFile1, inputFile2);
        
        ERMClient ermClient = ERMClient.getInstance();
        
        if (inputFile2 == null) {
        	if (FileUtil.isDirectory(inputFile1)) {
        		ermClient.configure(mainClass, 3, "jar/"+FileUtil.getFileName(jarPath), bucketName, FileUtil.getFileName(inputFile1));
        	} else {
        		ermClient.configure(mainClass, 3, "jar/"+FileUtil.getFileName(jarPath), bucketName, "input/"+FileUtil.getFileName(inputFile1));
        	}
        } else {
        	ermClient.configure(mainClass, 3, "jar/"+FileUtil.getFileName(jarPath), bucketName, "input/"+FileUtil.getFileName(inputFile1), "servidores/"+FileUtil.getFileName(inputFile2));
        }
        
        try {
			ermClient.runCluster();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
    }

	private static void prepareJobExecution(String bucketName, String jarPath, String inputFile1, String inputFile2)
			throws IOException {
		try {
        	S3Client s3Client = S3Client.getInstance();
        	
        	s3Client.createBucketIfNotExists(bucketName);
        	
        	s3Client.uploadFileIfNotExists(bucketName, "jar/", jarPath);
        	s3Client.uploadFileIfNotExists(bucketName, "input/", inputFile1);
        	s3Client.uploadFileIfNotExists(bucketName, "servidores/", inputFile2);
        	
        } catch (AmazonServiceException ase) {
            logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.error("Error Message:    " + ase.getMessage());
            logger.error("HTTP Status Code: " + ase.getStatusCode());
            logger.error("AWS Error Code:   " + ase.getErrorCode());
            logger.error("Error Type:       " + ase.getErrorType());
            logger.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        	logger.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
        	logger.error("Error Message: " + ace.getMessage());
        }
	}
}
