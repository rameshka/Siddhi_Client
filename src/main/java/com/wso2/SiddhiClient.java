package com.wso2;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class SiddhiClient {
    private Options option;
    private String amJar;
    private String priority;
    private String queue;
    private String amMemory;
    private String conMemory;
    private Configuration conf;
    private YarnClient yarnClient;
    private String deploymentJSON;


    private static final Logger logger = Logger.getLogger(SiddhiClient.class);
    private final String appMasterName ="SiddhiMaster";
    private final String appMasterDist = "SiddhiMaster.jar";
    private final String appMasterMainClass ="com.wso2.SiddhiMaster";
    private final String deplomentJSONDist = "deployment.json";


    public static void main(String[] args) {


        SiddhiClient siddhiClient = new SiddhiClient();

        try {

            siddhiClient.init(args);

                try {

                    if (siddhiClient.run()){
                        System.exit(0);
                    }


                    System.exit(1);

                } catch (IOException e) {

                    logger.error("IOException while running application: ",e);

                } catch (YarnException e) {

                    logger.error("Exiting the application, Yarn Error",e);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }

        } catch (ParseException e) {

            logger.error("Initialization failed",e);

            System.exit(1);
        }


    }


    /*Initialize Siddhi Client*/
    public SiddhiClient() {

        //configuring the yarn environment
        //Requirement: Cofiguration files in the classpath
        this.conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();

        //as yarnclient is another service
        yarnClient.init(conf);

        option = new Options();

        option.addOption("amJar", true, "ApplicationMaster jar file path");
        option.addOption("amMemory", true, "ApplicationMaster container Memory");
        option.addOption("deploymentJSON", true, "Deployment JSON file path");
        option.addOption("priority", true, "Application Master priority");
        option.addOption("queue", true, "Application Master Queue");
        option.addOption("conMemory",true,"Container Memory");


    }

    public void init(String[] args) throws ParseException {


        CommandLine cmdLine = new GnuParser().parse(option, args);

        if (args.length == 0)
        {
            throw new IllegalArgumentException("No arguments Specified");
        }

        amMemory = cmdLine.getOptionValue("amMemory","512"); //Not sure
        priority = cmdLine.getOptionValue("priority","0");
        queue = cmdLine.getOptionValue("queue","default");
        conMemory =cmdLine.getOptionValue("conMemory","512");


        if(!cmdLine.hasOption("amJar")){

            throw  new IllegalArgumentException("ApplicationMaster path not given");
        }

        amJar = cmdLine.getOptionValue("amJar");

        if(!cmdLine.hasOption("deploymentJSON")){

            throw  new IllegalArgumentException("Distributed SiddhiApp path not given");
        }

        deploymentJSON = cmdLine.getOptionValue("deploymentJSON");



    }

    public boolean run() throws IOException, YarnException, URISyntaxException {

        //submit command to run Yarn Application
        //starting the yarnClient service
        yarnClient.start();

        //send application request to RM and get the responded application
        YarnClientApplication appMaster = yarnClient.createApplication();


        //all the information needed by RM to run AM
        ApplicationSubmissionContext appMasterContext = appMaster.getApplicationSubmissionContext();


        ApplicationId appId = appMasterContext.getApplicationId();


        appMasterContext.setApplicationName(appMasterName);//hardcoded as it is for siddhi

        // all the information required by the NM to run the container
        //env, jar, memory,Local Resources, commands
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        //set Local Resources for Container
        Map<String,LocalResource> localResources = new HashMap<String,LocalResource>();
        FileSystem fs = FileSystem.get(conf);
        Path appMasterSrc = new Path(amJar);
        Path appMasterDestination = new Path(fs.getHomeDirectory(), appMasterDist);

        String appMasterURI = appMasterDestination.toUri().toString();

        //copy from local file system to file system
        fs.copyFromLocalFile(false, true, appMasterSrc, appMasterDestination);
        FileStatus destStatus = fs.getFileStatus(appMasterDestination);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(appMasterDestination));
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());
        localResources.put(appMasterDist, amJarRsrc);



        //add JSON file to the application master
        Path deploymentJSONPath = new Path(deploymentJSON);
        Path deploymentJSONDestionation  = new Path(fs.getHomeDirectory(),deplomentJSONDist);
        fs.copyFromLocalFile(false,true,deploymentJSONPath,deploymentJSONDestionation);
        FileStatus deploymentJSONSts = fs.getFileStatus(deploymentJSONDestionation);
        LocalResource deploymentJSONSrc = Records.newRecord(LocalResource.class);
        deploymentJSONSrc.setType(LocalResourceType.FILE);
        deploymentJSONSrc.setVisibility(LocalResourceVisibility.APPLICATION);
        deploymentJSONSrc.setResource(ConverterUtils.getYarnUrlFromPath(deploymentJSONDestionation));
        deploymentJSONSrc.setTimestamp(deploymentJSONSts.getModificationTime());
        deploymentJSONSrc.setSize(deploymentJSONSts.getLen());
        localResources.put(deplomentJSONDist,deploymentJSONSrc);


        amContainer.setLocalResources(localResources);

        //set environment
        Map<String ,String > env = new HashMap<String, String>();
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");


        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH))

        {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }

        env.put("CLASSPATH", classPathEnv.toString());
        amContainer.setEnvironment(env);




        //Launch applicationMaster

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);


        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java ");

        vargs.add("-Xmx" + amMemory + "m");

        vargs.add(appMasterMainClass);
        vargs.add("--conMemory "+ conMemory );



        vargs.add("1>" +ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" +ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        vargs.add("--deploymentJSON " +deplomentJSONDist);


        vargs.add("--appID " + appId);

        vargs.add("--appMasterURI " + appMasterURI);


        StringBuilder command = new StringBuilder();

        for (CharSequence str : vargs)
        {
            command.append(str).append(" ");
        }


        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);



         //Specify resource capability of the applicationMaster

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(Integer.parseInt(amMemory));
        appMasterContext.setResource(capability);

        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(Integer.parseInt(priority));
        appMasterContext.setPriority(pri);
        appMasterContext.setQueue(queue);

        //specify ContainerLaunchContext for ApplicationMasterContext
        appMasterContext.setAMContainerSpec(amContainer);




        yarnClient.submitApplication(appMasterContext);
        return serviceState(appId);
    }

    //check applicationMaster status
    public boolean serviceState(ApplicationId appId) throws IOException, YarnException {






        while (true) {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Thread sleep in monitoring loop interrupted");
            }




            ApplicationReport report = yarnClient.getApplicationReport(appId);


            logger.info("Got application report from ASM for" + ", appId="
                    + appId.getId() + ", clientToAMToken="
                    + report.getClientToAMToken() + ", appDiagnostics="
                    + report.getDiagnostics() + ", appMasterHost="
                    + report.getHost() + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState="
                    + report.getYarnApplicationState().toString()
                    + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());


            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    logger.info("Application has completed successfully.");
                    return true;
                }
                else {
                    logger.info("Application failed." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                logger.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                return false;
            }

        }
    }

}
