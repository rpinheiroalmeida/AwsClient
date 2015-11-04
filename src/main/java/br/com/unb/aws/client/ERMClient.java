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
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class ERMClient {

	private static Logger logger = Logger.getLogger(ERMClient.class);

	private static final String HADOOP_VERSION = "2.4.0";
	private static final String INSTANCE_TYPE = InstanceType.M1Medium.toString();
	private static final UUID RANDOM_UUID = UUID.randomUUID();
	private static final String AMI_VERSION = "3.10.0";

	private static final String EMR_DEFAULT_ROLE = "EMR_DefaultRole";
	private static final String EMR_EC2_DEFAULT_ROLE = "EMR_EC2_DefaultRole";
	private static final List<String> DONE_STATES = Arrays.asList(
			new String[]{JobFlowExecutionState.COMPLETED.name(),
					JobFlowExecutionState.FAILED.name(),
					JobFlowExecutionState.TERMINATED.name(),
					"TERMINATED_WITH_ERRORS"}
			);


	private static ERMClient ermClient = new ERMClient();
	private static AmazonElasticMapReduceClient emr;//TODO REMOVER STATIC

	private String mainClass;
	private int instanceCount = 1;
	private String hadoopJar;
	private String bucketName;
	private String input;
	private String input2;

	//	private static final String S3N_LOG_URI = "s3n://" + BUCKET_NAME + "/log/";	

	private ERMClient() {
		emr = new AmazonElasticMapReduceClient(new PropertiesFileCredentialsProvider("/home/rodrigo/git/MapReduce/WordCount/input/credentials.properties"));
		emr.setRegion(Region.getRegion(Regions.US_EAST_1));
	}

	public static ERMClient getInstance() {
		return ermClient;
	}

	public void configure(String mainClass, int instanceCount, String hadoopJar, String bucketName, String input) {
		this.mainClass = mainClass;
		this.instanceCount = instanceCount;
		this.hadoopJar = hadoopJar;
		this.bucketName = bucketName;
		this.input = input;
	}
	
	public void configure(String mainClass, int instanceCount, String hadoopJar, String bucketName, String input1, String input2) {
		this.mainClass = mainClass;
		this.instanceCount = instanceCount;
		this.hadoopJar = hadoopJar;
		this.bucketName = bucketName;
		this.input = input1;
		this.input2 = input2;
	}

	private String getFlowName() {
		return FileUtil.getFileName(mainClass) + "-" + RANDOM_UUID.toString();
	}

	private String getS3HadoopJar() {
		return String.format("s3n://%s/%s", this.bucketName, this.hadoopJar);
	}

	private String getS3LogUri() {
		return String.format("s3n://%s/log", this.bucketName);
	}

	private List<String> getJobArgs() {
		if (input2 == null) {
			return Arrays.asList(String.format("s3n://%s/%s", bucketName, input), String.format("s3n://%s/out/%s", bucketName, getFlowName()));
		} else {
			return Arrays.asList(String.format("s3n://%s/%s", bucketName, input), 
					String.format("s3n://%s/%s", bucketName, input2),
					String.format("s3n://%s/out/%s", bucketName, getFlowName()));
		}
	}

	public boolean isDone(String status) {
		return DONE_STATES.contains(status);
	}

	private JobFlowInstancesConfig configInstance() throws Exception {

		// Configure instances to use
		JobFlowInstancesConfig instance = new JobFlowInstancesConfig();
		instance.setHadoopVersion(HADOOP_VERSION);
		instance.setInstanceCount(instanceCount);
		instance.setMasterInstanceType(INSTANCE_TYPE);
		instance.setSlaveInstanceType(INSTANCE_TYPE);

		return instance;
	}


	public void runCluster() throws Exception {
		// Configure the job flow
		RunJobFlowRequest request = new RunJobFlowRequest(getFlowName(), configInstance());
		request.
		withJobFlowRole(EMR_EC2_DEFAULT_ROLE).
		withServiceRole(EMR_DEFAULT_ROLE).
		withVisibleToAllUsers(true).
		withAmiVersion(AMI_VERSION).
		withInstances(new JobFlowInstancesConfig()
				.withInstanceCount(instanceCount)
				.withTerminationProtected(false)
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withMasterInstanceType(INSTANCE_TYPE)
				.withSlaveInstanceType(INSTANCE_TYPE)).
		setLogUri(getS3LogUri());

		// Configure the Hadoop jar to use
		HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(getS3HadoopJar());
		jarConfig.setArgs(getJobArgs());
		jarConfig.setMainClass(this.mainClass);

		try {

			StepConfig enableDebugging = new StepConfig()
					.withName("Enable debugging")
					.withActionOnFailure("TERMINATE_JOB_FLOW")
					.withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

			StepConfig runJar =
					new StepConfig(getS3HadoopJar().substring(getS3HadoopJar().indexOf('/') + 1),
							jarConfig);

			request.setSteps(Arrays.asList(new StepConfig[]{enableDebugging, runJar}));

			Date dateCreation = new Date();
			//Run the job flow
			RunJobFlowResult result = emr.runJobFlow(request);
			logger.info("RunJobFlowResult: " + result);
			//Check the status of the running job
//			String lastState = "";
			boolean terminatedJob = false;

			while(!terminatedJob) {
				ListClustersResult clusters = emr.listClusters(new ListClustersRequest().withCreatedAfter(dateCreation));
				for (ClusterSummary clusterSummary : clusters.getClusters()) {
					if (clusterSummary.getId().equals(result.getJobFlowId()) 
							/*&& !lastState.equals(clusterSummary.getStatus().getState())*/ ) {
//						lastState = clusterSummary.getStatus().getState();
						logger.info(String.format("Cluster Id[%s], Cluster Name[%s], Cluster Status[%s]", 
								clusterSummary.getId(), clusterSummary.getName(), clusterSummary.getStatus()));

						ListStepsResult stepsResult = emr.listSteps(new ListStepsRequest().withClusterId(clusterSummary.getId()));
						for (StepSummary stepSummary : stepsResult.getSteps()) {
							StepStatus status = stepSummary.getStatus();
							logger.info(String.format("Job Id[%s], Job Name[%s], Status[%s] at %s.", stepSummary.getId(), stepSummary.getName(), status.getState(), new Date().toString(), status.getStateChangeReason().toString()));
						}
						
						if (isDone(clusterSummary.getStatus().getState())) {
							terminatedJob = true;
						}

					}
				}
				Thread.sleep(10000);
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
	}

}
