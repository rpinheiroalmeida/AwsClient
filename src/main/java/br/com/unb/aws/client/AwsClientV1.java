package br.com.unb.aws.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
public class AwsClientV1 {

	private static Logger logger = Logger.getLogger(AwsClientV1.class);
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3 = getS3Connection();

//        String bucketName = "mapreduce.bolsafamilia";
        String bucketName = "mapreduce.processamento.bolsafamilia";

        logger.info("===========================================");
        logger.info("Getting Started with Amazon S3");
        logger.info("===========================================\n");

        try {
//            createBucket(s3, bucketName);
//            createBucket(s3, bucketNameInput);
//            createBucket(s3, bucketNameOutput);
//            createBucket(s3, bucketNameLog);

//            uploadFile(s3, bucketName, "input/input.csv", "/home/rodrigo/git/MapReduce/WordCount/input/input.csv");
//            uploadFile(s3, bucketName, "jar/tarefa.jar", "/home/rodrigo/git/MapReduce/WordCount/target/wordcount-0.0.1-SNAPSHOT.jar");
            
            uploadFile(s3, bucketName, "jar/ProcessamentoBolsaFamilia-0.0.1-SNAPSHOT.jar", "/home/rodrigo/workspace/ProcessamentoBolsaFamilia/target/ProcessamentoBolsaFamilia-0.0.1-SNAPSHOT.jar");
//            uploadFile(s3, bucketName, "input", "/home/rodrigo/hadoop/input/201501_BolsaFamiliaFolhaPagamento.csv");
            uploadFile(s3, bucketName, "input/servidores/20150131_Honorarios.csv", "/home/rodrigo/hadoop/input/servidores/20150131_Honorarios.csv");
            
            listObjectsFromBucket(s3, bucketName);

//            deleteBucket(s3, bucketName);
//            deleteBucket(s3, bucketNameInput);
//            deleteBucket(s3, bucketNameOutput);
//            deleteBucket(s3, bucketNameLog);
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

	private static void deleteBucket(AmazonS3 s3, String bucketName) {
		/*
         * Delete a bucket - A bucket must be completely empty before it can be
         * deleted, so remember to delete any objects from your buckets before
         * you try to delete them.
         */
        System.out.println("Deleting bucket " + bucketName + "\n");
        s3.deleteBucket(bucketName);
	}
	
	private static void listObjectsFromBucket(AmazonS3 s3, String bucketName) {
		/*
		 * List objects in your bucket by prefix - There are many options for
		 * listing the objects in your bucket.  Keep in mind that buckets with
		 * many objects might truncate their results when listing their objects,
		 * so be sure to check if the returned object listing is truncated, and
		 * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
		 * additional results.
		 */
		logger.info("Listing objects");
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
		        .withBucketName(bucketName));
//		        .withPrefix("My"));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
		    logger.info(" - " + objectSummary.getKey() + "  " +
		            "(size = " + objectSummary.getSize() + ")");
		}
	}

	private static void uploadFile(AmazonS3 s3, String bucketName, String key,String filePath) throws IOException {
		/*
		 * Upload an object to your bucket - You can easily upload a file to
		 * S3, or upload directly an InputStream if you know the length of
		 * the data in the stream. You can also specify your own metadata
		 * when uploading to S3, which allows you set a variety of options
		 * like content-type and content-encoding, plus additional metadata
		 * specific to your applications.
		 */
		logger.info("Uploading a new object to S3 from a file\n");
//		s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));
		if (!isExistS3(s3, bucketName, key)) {
			s3.putObject(new PutObjectRequest(bucketName, key, new File(filePath)));
		}
		
		
		
//		 Create a list of UploadPartResponse objects. You get one of these for
//		 each part upload.
//		List<PartETag> partETags = new ArrayList<PartETag>();
//
//		// Step 1: Initialize.
//		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
//		                                                    bucketName, key);
//		InitiateMultipartUploadResult initResponse = 
//		                              s3.initiateMultipartUpload(initRequest);
//
//		File file = new File(filePath);
//		long contentLength = file.length();
//		long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.
//
//		try {
//		    // Step 2: Upload parts.
//		    long filePosition = 0;
//		    for (int i = 1; filePosition < contentLength; i++) {
//		        // Last part can be less than 5 MB. Adjust part size.
//		    	partSize = Math.min(partSize, (contentLength - filePosition));
//		    	
//		        // Create request to upload a part.
//		        UploadPartRequest uploadRequest = new UploadPartRequest()
//		            .withBucketName(bucketName).withKey(key)
//		            .withUploadId(initResponse.getUploadId()).withPartNumber(i)
//		            .withFileOffset(filePosition)
//		            .withFile(file)
//		            .withPartSize(partSize);
//
//		        // Upload part and add response to our list.
//		        partETags.add(s3.uploadPart(uploadRequest).getPartETag());
//
//		        filePosition += partSize;
//		    }
//
//		    // Step 3: Complete.
//		    CompleteMultipartUploadRequest compRequest = new 
//		                CompleteMultipartUploadRequest(bucketName, 
//		                                               key, 
//		                                               initResponse.getUploadId(), 
//		                                               partETags);
//
//		    s3.completeMultipartUpload(compRequest);
//		} catch (Exception e) {
//		    s3.abortMultipartUpload(new AbortMultipartUploadRequest(
//		              bucketName, key, initResponse.getUploadId()));
//		}
	}
	
	private static void createBucket(AmazonS3 s3, String bucketName) {
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
			mkdir(s3, bucketName, "input/");
			mkdir(s3, bucketName, "output/");
			mkdir(s3, bucketName, "log/");
		} catch(AmazonServiceException e) {
			if (e.getStatusCode() == 409 ) {
				logger.info("Bucket exists");
			} else {
				throw e;
			}
		} finally {
			mkdir(s3, bucketName, "input/");
			mkdir(s3, bucketName, "output/");
			mkdir(s3, bucketName, "log/");
		}
	}
	
	private static void mkdir(AmazonS3 s3, String bucketName, String folderName) {
		 // create meta-data for your folder and set content-length to 0
	    ObjectMetadata metadata = new ObjectMetadata();
	    metadata.setContentLength(0);

	    // create empty content
	    InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

	    // create a PutObjectRequest passing the folder name suffixed by /
	    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
	                folderName, emptyContent, metadata);
	    
	    // send request to S3 to create folder
	    s3.putObject(putObjectRequest);
	}

	private static AmazonS3 getS3Connection() {
		/*
         * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users) 
         * and save the following lines after replacing the underlined values with your own.
         *
         * [default]
         * aws_access_key_id = YOUR_ACCESS_KEY_ID
         * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
         */

        AmazonS3 s3 = new AmazonS3Client();
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);
		return s3;
	}

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }
    
    public static boolean isExistS3(AmazonS3 s3Client, String bucketName, String file) {
        ObjectListing objects = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(file));

        for (S3ObjectSummary objectSummary: objects.getObjectSummaries()) {
            if (objectSummary.getKey().equals(file)) {
                return true;
            }
        }
        return false;
    }

}
