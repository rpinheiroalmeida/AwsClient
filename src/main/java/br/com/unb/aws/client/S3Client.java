package br.com.unb.aws.client;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 */
public class S3Client {

	private static Logger logger = Logger.getLogger(S3Client.class);

    private static final S3Client s3Client = new S3Client();
    
    private AmazonS3 s3; 
    
    private S3Client() {
    	s3 = getS3Connection();
    }
    
    public static S3Client getInstance() {
    	return s3Client;
    }
    
    public void createBucketIfNotExists(String bucketName) {
    	if (!isExistS3(bucketName)) {
    		createBucket(bucketName);
    	}
	}
    
    private void createBucket(String bucketName) {
		/*
		 * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
		 * so once a bucket name has been taken by any user, you can't create
		 * another bucket with that same name.
		 *
		 * You can optionally specify a location for your bucket if you want to
		 * keep your data closer to your applications or users.
		 */
		try {
			logger.info("Creating bucket " + bucketName + "\n");
			s3.createBucket(bucketName);
		} catch(AmazonServiceException ase) {
			logger.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
		} 
	}
    
    private boolean isExistS3(String bucketName) {
    	
    	try {
    		ObjectListing objects = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
    		
    		for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
    			if (objectSummary.getKey().equals(bucketName)) {
    				return true;
    			}
    		}
    		return false;
    	} catch (AmazonServiceException ase) {
    		return false;
        } 
    }
    
    private AmazonS3 getS3Connection() {
		/*
         * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users) 
         * and save the following lines after replacing the underlined values with your own.
         *
         * [default]
         * aws_access_key_id = YOUR_ACCESS_KEY_ID
         * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
         */

        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
		return s3;
	}

	public void uploadFileIfNotExists(String bucketName, String dest, String filePath) throws IOException {
		if (filePath != null) {
			if (FileUtil.isDirectory(filePath)) {
				File[] files = FileUtil.getFiles(filePath);
				for (File file : files) {
					uploadFile2S3(bucketName, dest, file.getAbsolutePath());
				}
			} else {
				uploadFile2S3(bucketName, dest, filePath);
			}
		}
	}

	private void uploadFile2S3(String bucketName, String dest, String filePath) throws IOException {
		String key = dest + FileUtil.getFileName(filePath);
		if (!isExistS3(bucketName, key)) {
			uploadFile(bucketName, key, filePath);
		}
	}
	
	private boolean isExistS3(String bucketName, String file) {
		
		try {
			ObjectListing objects = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(file));
			
			for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
				if (objectSummary.getKey().equals(file)) {
					return true;
				}
			}
			return false;
		} catch (AmazonServiceException ase) {
			return false;
        }
    }
	
	public void uploadFile(String bucketName, String key,String filePath) throws IOException {
		/*
		 * Upload an object to your bucket - You can easily upload a file to
		 * S3, or upload directly an InputStream if you know the length of
		 * the data in the stream. You can also specify your own metadata
		 * when uploading to S3, which allows you set a variety of options
		 * like content-type and content-encoding, plus additional metadata
		 * specific to your applications.
		 */
		logger.info(String.format("Uploading a new object to S3 from a file [%s]", FileUtil.getFileName(filePath)));
		s3.putObject(new PutObjectRequest(bucketName, key, new File(filePath)));
	}
	

}
