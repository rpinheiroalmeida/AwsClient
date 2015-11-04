package br.com.unb.aws.client;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionState;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class ERMClientV1 {

	private static Logger logger = Logger.getLogger(ERMClientV1.class);
	
	private static final String HADOOP_VERSION = "2.6.0";
	private static final int INSTANCE_COUNT = 1;
	private static final String INSTANCE_TYPE = InstanceType.M1Small.toString();
	
	private static final UUID RANDOM_UUID = UUID.randomUUID();
	private static final String FLOW_NAME = "out-" + RANDOM_UUID.toString();
	private static final String BUCKET_NAME = "mapreduce.processamento.bolsafamilia";
	
	private static final String S3N_HADOOP_JAR = "s3n://" + BUCKET_NAME + "/jar/wordcount-0.0.1-SNAPSHOT.jar";
	private static final String S3N_LOG_URI = "s3n://" + BUCKET_NAME + "/log/";
//	s3://mapreduce.bolsafamilia/input s3://mapreduce.bolsafamilia/out
	
	private static final String[] JOB_ARGS =
			new String[]{"s3n://" + BUCKET_NAME + "/input",
					"s3n://" + BUCKET_NAME + "/result/" + FLOW_NAME};
	
	private static final List<String> ARGS_AS_LIST = Arrays.asList(JOB_ARGS);

	private static final List<JobFlowExecutionState> DONE_STATES = Arrays
			.asList(new JobFlowExecutionState[]{JobFlowExecutionState.COMPLETED,
					JobFlowExecutionState.FAILED,
					JobFlowExecutionState.TERMINATED});
	static AmazonS3 s3;
	static AmazonElasticMapReduceClient emr;

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
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);
		return s3;
	}
	private static void init() throws Exception {
		s3 = getS3Connection();
		emr = new AmazonElasticMapReduceClient(new PropertiesFileCredentialsProvider("/home/rodrigo/git/MapReduce/WordCount/input/credentials.properties"));
		emr.setRegion(Region.getRegion(Regions.US_EAST_1));
	}

	private static JobFlowInstancesConfig configInstance() throws Exception {

		// Configure instances to use
		JobFlowInstancesConfig instance = new JobFlowInstancesConfig();
		instance.setHadoopVersion(HADOOP_VERSION);
		instance.setInstanceCount(INSTANCE_COUNT);
		instance.setMasterInstanceType(INSTANCE_TYPE);
		instance.setSlaveInstanceType(INSTANCE_TYPE);
		// instance.setKeepJobFlowAliveWhenNoSteps(true);
		// instance.setEc2KeyName("4synergy_palma");

		return instance;
	}
	
	private static void runCluster() throws Exception {
		// Configure the job flow
		RunJobFlowRequest request = new RunJobFlowRequest(FLOW_NAME, configInstance());
		request.
				withJobFlowRole("EMR_EC2_DefaultRole").
				withServiceRole("EMR_DefaultRole").
				withVisibleToAllUsers(true).
				withAmiVersion("2.4.9").
				withInstances(new JobFlowInstancesConfig()
//				           .withEc2KeyName("mapreduce")
				           .withInstanceCount(3)
				           .withTerminationProtected(false)
				           .withKeepJobFlowAliveWhenNoSteps(false)
				           .withMasterInstanceType("m1.medium")).
//				           .withSlaveInstanceType("m1.large")).
				setLogUri(S3N_LOG_URI);

		// Configure the Hadoop jar to use
		HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(S3N_HADOOP_JAR);
		jarConfig.setArgs(ARGS_AS_LIST);

		try {

			StepConfig enableDebugging = new StepConfig()
					.withName("Enable debugging")
					.withActionOnFailure("TERMINATE_JOB_FLOW")
					.withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

			StepConfig runJar =
					new StepConfig(S3N_HADOOP_JAR.substring(S3N_HADOOP_JAR.indexOf('/') + 1),
							jarConfig);

			request.setSteps(Arrays.asList(new StepConfig[]{enableDebugging, runJar}));

			//Run the job flow
			RunJobFlowResult result = emr.runJobFlow(request);

			logger.info("RunJobFlowResult: " + result);
			
			//Check the status of the running job
			String lastState = "";
			
			STATUS_LOOP:
				while(true) {
					ListClustersResult clusters = emr.listClusters();
					logger.info("Clusters: " + clusters);
					for (ClusterSummary clusterSummary : clusters.getClusters()) {
						logger.info("Cluster Summary: " + clusterSummary);
//						ClusterStatus clusterStatus = clusterSummary.getStatus();
//						logger.info("Cluster Status: " + clusterStatus);
//						if (clusterStatus.getState().equals("TERMINATING" )) {
//							break STATUS_LOOP;
//						}
						
						ListStepsResult stepsResult = emr.listSteps(new ListStepsRequest().withClusterId(clusterSummary.getId()));
						for (StepSummary stepSummary : stepsResult.getSteps()) {
							logger.info("Step Summary: " + stepSummary.toString());
							StepStatus status = stepSummary.getStatus();
							if (isDone(status.getState())) {
								logger.info(String.format("Job %s : %s", status.getState(), stepSummary.getName()));
								break STATUS_LOOP;
							} else if (!lastState.equals(status)) {
								lastState = status.getState();
								logger.info(String.format("Job %s at %s", status.getState(), new Date().toString()));
							}
						}
					}
					Thread.sleep(10000);
				}
//
//			STATUS_LOOP:
//				while (true) {
//					DescribeJobFlowsRequest desc =
//							new DescribeJobFlowsRequest(
//									Arrays.asList(new String[]{result.getJobFlowId()}));
//					DescribeJobFlowsResult descResult = emr. describeJobFlows(desc);
//					for (JobFlowDetail detail : descResult.getJobFlows()) {
//						String state = detail.getExecutionStatusDetail().getState();
//						if (isDone(state)) {
//							System.out.println("Job " + state + ": " + detail.toString());
//							break STATUS_LOOP;
//						} else if (!lastState.equals(state)) {
//							lastState = state;
//							System.out.println("Job " + state + " at " + new Date().toString());
//						}
//					}
//					Thread.sleep(10000);
//				}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
	}

	public static boolean isDone(String status) {
//		JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
		return DONE_STATES.contains(status);
	}

	public static void main(String[] args) {
		try {
			init();
			runCluster();
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
}
