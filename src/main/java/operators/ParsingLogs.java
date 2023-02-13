package operators;

import datatypes.counter.Counter;
import datatypes.input.Input;
import utils.MathUtils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static coordinator.CoordinatorFunction.*;
import static operators.StrToCounter.getQueries;
import static operators.StrToCounter.strToCounter;
import static state.State.*;
import static utils.MathUtils.*;
import static utils.MathUtils.calculateCondition;
import static worker.WorkerFunction.getKey;


public class ParsingLogs {

    public static void main(String[] args) throws Exception {


        // This class used for error relative to MLE experiment
        // collectMessages("alarm","NON_UNIFORM");
        // collectMessages("link","UNIFORM");
        // collectMessages("link","EXACT");
        // collectMessages("hepar2","RC");
        // collectMessages("alarm","RC");
        // collectMessages("sachs","NON_UNIFORM");
        // collectMessagesFGM("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\alarm\\Communication cost-Number of sites\\FGM\\500K\\UNIFORM\\parallelism = 10\\coordinator_stats.txt");
        // collectMessagesFile("","",2,"RC");

        // long condition = calculate(8,0.1/210,2);
        // double power = highestPowerOf2(0.1/210,condition+10,8,2);
        // if(power < 1d) power=0;


        // Change highestPowerOf2 , DONE
        // Avoid two-way communication - Check - RC(DONE),DC(DONE)
        // Conditions on RC(DONE),DC(DONE)
        // numMessagesWorkers IND , coordinator side , DONE
        // END_OF_WORKER messages - Check , DONE
        // SubroutineDouble - Check , DONE
        // Overhead(RC) - Check - DONE, Overhead(DC) - DONE
        // Queries stats - Counter zero true parameters per query - Check
        // Print timer WorkerState and CoordinatorState - Check - DONE
        // Change sketches(epsilon,delta) - Check , DONE
        // SafeZone change - Check - DONE
        // Update worker and coordinator state(IND,FGM) - Check - DONE
        // Check lastTs on counters from Workers and Coordinator - Check - DONE
        // Timer for throughput,runtime - Check - 100ms - DONE
        // HEPAR2 all the folders/diagrams(all) - Check - DONE
        // Choose throughput(collectStatsRuns) - Check - DONE
        // JAR,Experiment cluster - Check - DONE
        // Experiment config - Check - DONE
        // Clean up the code - Check


        // Disable workerStats
        // Communication cost vs Training instances
        // Datasets : 5K,50K,500K,5M
        // Epsilon = 0.1 , delta = 0.25
        // Workers = 16, parallelism = 8 or 32(local)
        // Source partition = 16, parallelism = 1
        // Feedback partition = 2, parallelism = 2
        // Coordinator parallelism = 1 or 32(local)
        // Number of runs = 5
        // Comments : RC - DONE , DC - DONE , FGM - DONE(Error to EXACT_MLE)
        // Comments : 50M - DONE,Dummy Father-50M - DONE,Laplace Smoothing
        // FGM > DC > RC

        // Disable workerStats
        // Communication Cost VS epsilon
        // Datasets : 100K,500K,1M,5M
        // Epsilon = [0.02,0.12] , delta = 0.25
        // Workers = 16, parallelism = 8 or 32(local)
        // Source partition = 16, parallelism = 1
        // Feedback partition = 2 , parallelism = 2
        // Coordinator parallelism = 1 or 32(local)
        // Number of runs = 5
        // Comments : RC - DONE , DC - DONE , FGM - DONE
        // FGM > RC ~= DC

        // Disable workerStats
        // Communication Cost VS number of sites
        // Datasets : 500K
        // Epsilon = 0.1 , delta = 0.25
        // Workers = [2,26] , parallelism = 8 or 32(local)
        // Source partition = [2,26], parallelism = 1
        // Feedback partition = 2, parallelism = 2
        // Coordinator parallelism = 1 or 32(local)
        // Number of runs = 5
        // Comments : RC - DONE , DC - DONE , FGM - DONE
        // Comments : RC vs DC for workers = [26,60] => 32,38,44,52,60,64 - DONE
        // RC > FGM ~= DC

        // Throughput(events/sec)-Runtime VS number of sites
        // Datasets : 500K
        // Epsilon = 0.1 , delta = 0.25
        // Workers = [2,10] , parallelism = [2,10]
        // Source partition = [2,10], parallelism = [2,10]
        // Feedback partition = 2, parallelism = 2
        // Coordinator parallelism = 1
        // Number of runs = 5
        // Comments : RC - DONE , DC - DONE , EXACT - DONE, FGM - DONE
        // Comments : RC - DONE , DC - DONE , with initial condition - DONE
        // Comments : Throughput/Tuples vs Processing Time - DONE
        // FGM > DC > RC

        // Throughput(events/sec)-Runtime VS cluster size
        // Datasets : 500K
        // Epsilon = 0.1 , delta = 0.25
        // Parallelism = [2,16]
        // Workers =  16, parallelism
        // Source partition = 16, parallelism = [2,16]
        // Feedback partition = 2, parallelism = 2
        // Coordinator parallelism = 1
        // Number of runs = 5
        // Comments : RC - DONE , DC - DONE , EXACT - DONE, FGM - DONE
        // Comments : RC - DONE , DC - DONE , with initial condition - DONE
        // FGM > DC > RC

        // Network with one counter - Check epsilon,two-way communication
        // Keep the counters of type of messages on Coordinator

        // collectStatBN("hepar2",0.1,0.25,false);
        // collectStatsSchema(60,0.02);
        // collectStatsEpsilon(70,8,0.12,0.25);
        collectStatsCounters(100,0.1/(210),0.25);



        String file1 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\workers\\RANDOMIZED\\BASELINE\\workers=6";
        String file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\workers\\RANDOMIZED\\BASELINE\\workers=8";
        // checkWorkers(file1,6,file2,8,"");
        // file1 = "C:\\Users\\Nikos\\Desktop\\test\\alarm_results\\dummy_father_alarm\\datasets\\";
        // collectMissingFiles(file1,"dataset",6,16,8,false);
        // readFile();


        file1 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\epsilon_1\\EXACT";
        file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\epsilon_1\\RANDOMIZED\\BASELINE\\eps=0.10";
        // checkEpsilon(file1,8,file2,8,"");



        String file = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\workers\\";
        String exactFile = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\workers\\";
        // collectWorkers(file,file,exactFile);
        // collectWorkersRuns(file,file,exactFile,5,false);
        // collectStatsRuns(file,file,"workers",5);
        // collectAggregatedStatsRuns(file,file,"workers");
        // collectStatsRun(file,exactFile,"workers");

        file = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\workers26\\workers\\";
        exactFile = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\workers\\";
        // collectWorkersRuns(file,file,exactFile,6,false);
        // collectStatsRuns(file,file,"workers",5,false);
        // collectAggregatedStatsRuns(file,file,"workers");

        file = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\epsilon\\5M\\";
        exactFile = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\epsilon\\EXACT\\5M\\";
        // collectEpsilonRuns(file,file,exactFile,5,16,false);
        // collectStatsRuns(file,file,"epsilon",5,false);
        // collectAggregatedStatsRuns(file,file,"epsilon");

        file = "C:\\Users\\Nikos\\Desktop\\test\\link_results\\dummy_father_link\\datasets\\";
        exactFile = "C:\\Users\\Nikos\\Desktop\\test\\link_results\\link\\datasets\\";
        // collectDatasets(file,file,exactFile,16,false);
        // collectDatasetRuns(file,file,exactFile,6,16,false);
        // collectStatsRuns(file,file,"datasets",5,false);
        // collectAggregatedStatsRuns(file,file,"datasets");
        // collectStatsRun(file,file,"dataset");

        file = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\parallelism\\par_sccparconst\\parallelism\\";
        exactFile = "C:\\Users\\Nikos\\Desktop\\test\\hepar2_results\\parallelism\\par_sccparconst\\parallelism\\";
        // collectParallelism(file,file,exactFile,5,16,false);
        // collectParallelismRuns(file,file,exactFile,5,16,true);
        // collectStatsRuns(file,file,"parallelism",5,true);
        // collectAggregatedStatsRuns(file,file,"parallelism");


        file1 = "C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\500K\\result_3_RC_B";
        String file3 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-DC\\500K\\Experiment_1\\BASELINE";
        file3 = "C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\500K\\result_4_RC_B";
        file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\epsilon\\EXACT";
        //file2 = "C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\500K\\EXACT";
        double x = (0.1d/210);
        long cond = MathUtils.calculateCondition(0.1/210,31614,8);
        // file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-DC\\50K\\Experiment_1\\EXACT";
        // checkQueries(file1,8,file2,8,"");
        // checkDC(file1,8,file2,8,file3,8,"");
        file1 = "C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring";
        file2 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\epsilon\\EXACT";
        file3 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\hepar2_500K\\epsilon\\RANDOMIZED\\BASELINE\\eps=0.10";
        // checkRC(file1,8,file2,8,file3,8,"");
        // checkRCDC(file1,8,file2,8,file3,8,"");

        file1 = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\test\\";
        file2 = "C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\5M\\EXACT";
        // collectMessagesFGMFile(file1,file1,22550);
        // getErrorEXACTFile(file1,file2,file1,0.1d);
        // getRuntimeFile(file1,file1);
        // getThroughputFile(file1,file1,8);
        // getTuplesProcFile(file1,file1,8);
        // getTuplesFile(file1,file1,8);

        // compare("hepar2","500K","BASELINE");
        // collectMessages("","",8);
        // System.out.println("End compare !!!!\n");

        // discardEnd();

        // compareSchemas(8);
        // testOrdering(1);
        // getTuplesProc(8,"");
        // getRuntime(8,"UNIFORM");
        // getThroughput(8,"BASELINE");
        // getTuples(2,"BASELINE");
        // compare("hepar2","Experiment_1","50M");

    }



    // Read file and get the errors to GT as string line
    public static void getError(BufferedReader br1) throws IOException {

        String line1 = br1.readLine();
        int querySize = 0;
        double avg_abs_error_truth = 0;
        double avg_error_truth = 0;
        double tmp_error = 0;
        String errors = "";
        String errors_runs = "";
        String errors_runs_200 = "";
        String gt_prob = "";

        while ( line1 != null ) {

            line1 = line1.replaceAll("\0","");
            if( !line1.contains("Input") ){ line1 = br1.readLine(); }

            if(  line1 != null && line1.contains("Input") ){

                // Process the inputs from the two files

                // Split the lines based on colon
                String[] splitsLine1 = line1.split(":");
                String error = null; String abs_error = null;String gtProb = null;

                // For the first line
                for(int i=0;i<splitsLine1.length;i++){
                    if(splitsLine1[i].contains("error") && !splitsLine1[i].contains("absolute") ) error =  splitsLine1[i+1];
                    if(splitsLine1[i].contains("absolute error")) abs_error =  splitsLine1[i+1];
                    if(splitsLine1[i].contains("truth probability")) gtProb = splitsLine1[i+1];
                }

                // Get the ground truth probability
                gt_prob = gt_prob.concat(gtProb.split(",")[0].trim()+",");

                // Find the error between two lines(estimated-exact)
                double errorTruth = Double.parseDouble(error.split(",")[0]);
                double absErrorTruth = Double.parseDouble(abs_error.split(",")[0]);
                avg_error_truth += errorTruth;
                avg_abs_error_truth += absErrorTruth;

                // Append the error
                errors = errors.concat(errorTruth +",");

                // Read the next lines
                line1 = br1.readLine();
                querySize++;

                // Append the avg error per 200 queries and calculate the avg error per 200 queries
                tmp_error += errorTruth;
                if( querySize != 0 && querySize % 200 == 0 ){
                    tmp_error = tmp_error/200;
                    errors_runs = errors_runs.concat(tmp_error+",");
                    errors_runs_200 = errors_runs_200.concat((avg_error_truth / querySize) +",");
                    tmp_error = 0;
                }
            }

        }

        System.out.println("Error to ground truth");
        System.out.println( "Ground truth probability : " + gt_prob.substring(0,gt_prob.length()-1));
        System.out.println( "Errors : " + errors.substring(0,errors.length()-1));
        System.out.println( "Errors runs : " + errors_runs.substring(0,errors_runs.length()-1));
        System.out.println( "Errors runs 200 : " + errors_runs_200.substring(0,errors_runs_200.length()-1));
        System.out.println( "Average absolute error : " + avg_abs_error_truth/querySize);
        System.out.println( "Average error : " + avg_error_truth/querySize);
        System.out.println("Queries size : " + querySize);
        System.out.println("End!!!");
    }

    // Read files and get the errors to EXACTMLE as string line
    public static void getErrorEXACTMLE(String inputFile,String exactFile) throws IOException {

        BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\queries1")); // input file as args[0]
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\sachs\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\queries1")); // input file as args[0]
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\link\\Error_to_MLE-RC\\50K\\Experiment_1\\queries1")); // input file as args[0]
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Extensions\\FGM\\500K\\Experiment_1\\queries1"));

        // Get the errors as string line(for one file)
        getError(br1);

        // Second input-Exact counter
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries2")); // input file as args[0]
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_GT_eps\\50K\\DC\\queries2"));
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\sachs\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\queries2")); // input file as args[0]
        BufferedReader br2 = new BufferedReader(new FileReader(exactFile+"\\queries2")); // input file as args[0]
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\link\\Error_to_MLE-RC\\50K\\Experiment_1\\queries2")); // input file as args[0]
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Extensions\\FGM\\500K\\Experiment_1\\queries2"));

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter("errorMLE"));

        br1 = new BufferedReader(new FileReader(inputFile+"\\queries1")); // To work with getError function
        String line1 = br1.readLine();
        String line2 = br2.readLine();
        int querySize = 0;
        double error = 0d;
        double avg_abs_error = 0d;
        int lbv = 0,ubv = 0;

        String errors = "";
        String errors_runs = "";
        String errors_runs_200 = "";
        double tmp_error = 0;

        while ( line1 != null && line2 != null ) {

            line1 = line1.replaceAll("\0","");
            line2 = line2.replaceAll("\0","");

            if( !line1.contains("Input") ){ line1 = br1.readLine(); }
            if( !line2.contains("Input") ){ line2 = br2.readLine(); }

            if(  line1 != null && line2 != null && line1.contains("Input") && line2.contains("Input") ){

                // Process the inputs from the two files

                // Split the lines based on colon
                String[] splitsLine1 = line1.split(":");
                String value1 = null; String estProb1 = null; String truthProb1 = null;

                String[] splitsLine2 = line2.split(":");
                String value2 = null; String estProb2 = null; String truthProb2 = null;

                // For the first line
                for(int i=0;i<splitsLine1.length;i++){
                    if(splitsLine1[i].contains("value")) value1 = splitsLine1[i+1];
                    if(splitsLine1[i].contains("estimated probability")) estProb1 =  splitsLine1[i+1];
                    if(splitsLine1[i].contains("truth probability")) truthProb1 =  splitsLine1[i+1];
                }

                // For the second line
                for(int i=0;i<splitsLine2.length;i++){
                    if(splitsLine2[i].contains("value")) value2 = splitsLine2[i+1];
                    if(splitsLine2[i].contains("estimated probability")) estProb2 = splitsLine2[i+1];
                    if(splitsLine1[i].contains("truth probability")) truthProb2 =  splitsLine1[i+1];
                }

                // Check the value
                value1 = value1.trim(); value2 = value2.trim();
                if(!value1.equals(value2)){
                    System.out.println("Count : "+querySize+" , queries are not equal");
                    return;
                }

                // Check ground truth probabilities
                if(Double.parseDouble(truthProb1.split(",")[0]) - Double.parseDouble(truthProb2.split(",")[0]) != 0){
                    System.out.println("Count : "+querySize+" , queries are not equal");
                    return;
                }

                // Find the error between two lines(estimated-exact)
                double estProb = Double.parseDouble(estProb1.split(",")[0]);
                double exactProb = Double.parseDouble(estProb2.split(",")[0]);
                double errorLine = estProb - exactProb;
                System.out.println((querySize+1)+" )" + " -0.1 <= ln(estimated) - ln(truth) = " + (estProb-exactProb) + " <= 0.1" );
                writer.write((querySize+1)+" )" + " -0.1 <= ln(estimated) - ln(truth) = " + (estProb-exactProb) + " <= 0.1\n");

                // Check the bounds
                // Up to two decimal places precession : Precision.equals(0.104d,0.1d,0.01);
                if( (estProb-exactProb) >= 0.1d ){ ubv++; }
                else if( (estProb-exactProb) <= -0.1d ){ lbv++; }

                // Append the error
                errors = errors.concat(errorLine +",");

                // Update the error
                error += errorLine;
                avg_abs_error += Math.abs(errorLine);

                // Update
                querySize++;

                // Append the avg error per 200 queries and calculate the avg error per 200 queries
                tmp_error += errorLine;
                if( querySize != 0 && querySize % 200 == 0 ){
                    tmp_error = tmp_error/200;
                    errors_runs = errors_runs.concat(tmp_error+",");
                    errors_runs_200 = errors_runs_200.concat((error / querySize) +",");
                    tmp_error = 0;
                }

                // Read the next lines
                line1 = br1.readLine();
                line2 = br2.readLine();
            }

        }

        System.out.println("Error to EXACTMLE");
        System.out.println("Queries size : " + querySize);
        writer.write("Queries size : " + querySize + "\n");

        System.out.println( "Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv);
        writer.write("Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv + "\n");
        writer.write( "Average absolute error : " + avg_abs_error/querySize);

        System.out.println( "\nAverage absolute error : " + avg_abs_error/querySize);
        System.out.println( "Errors : " + errors.substring(0,errors.length()-1));
        System.out.println( "Errors runs : " + errors_runs.substring(0,errors_runs.length()-1));
        System.out.println( "Errors runs 200 : " + errors_runs_200.substring(0,errors_runs_200.length()-1));


        // Close buffers
        br1.close();
        br2.close();
        writer.close();
    }

    // Read file and collect the number of messages for RC
    public static long collectMessages(String file) throws IOException {

        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long overheadMessages = 0; // overhead of messages to Coordinator
        int zeroCountersCord = 0; // Counter with zero value at Coordinator
        int counterProb = 0; // The total number of counters with probability=1
        // For coordinator file the numMessages = num_round of each Counter - RANDOMIZED case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split(":");
                String sent = splits[splits.length-2].substring(0,splits[splits.length-2].lastIndexOf("o")).trim();

                numMessagesSent = Long.parseLong(sent);
                overheadMessages = Long.parseLong(splits[splits.length-1].trim());
            }

            // if(line.trim().equals("Processing BN queries : 1000")) break;

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("Coordinator state") || line.contains("Worker state ")){
                find = true;
                line = br.readLine();
                continue;
            }

            if(find && line.contains("Counter")){

                // Process the counter

                // Split the lines based on colon
                String[] splitsLine = line.split(",");
                String message = null;

                for (String split : splitsLine) {
                    if (split.contains("numMessages")){
                        message = split.split(":")[1].trim();
                        if(Long.parseLong(message) == 0) messagesZero++;
                    }
                    if(split.contains("counter ")){
                        if(Integer.parseInt(split.split(":")[1].trim()) == 0) zeroCounters += 1;
                    }
                    if(split.contains("last sent values")){
                        // Get the values
                        String values = split.split(":")[1].trim();
                        // Only for coordinator
                        if(!values.contains("null")){
                            String[] workersValues = ((line.split("\\{")[1]).split("}")[0]).trim().split(",");
                            int zero = 0;
                            for(String value : workersValues){ if(Integer.parseInt(value.split("=")[1]) == 0 ) zero++; }
                            if(zero == workersValues.length) zeroCountersCord++;
                        }
                    }
                    if(split.contains("prob")){
                        if(Double.parseDouble(split.split(":")[1].trim()) == 1.0d) counterProb += 1;
                    }
                }
                
                // Update the messages
                messages += Long.parseLong(Objects.requireNonNull(message));

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            System.out.println( "Total counters : " + totalCount
                                + " , Total number of rounds for all counters : " +messages
                                + " , Total number of counters with zero rounds : " + messagesZero
                                + " , Zero counters : " + zeroCountersCord
                                + " , numMessagesSent : " + numMessagesSent
                                + " , overheadMessages : " + overheadMessages
                                + " , Total number of counters with prob=1 : " + counterProb);
        }
        // Workers
        else {

            System.out.println("Total counters : " + totalCount
                    + " , Total messages : " + messages
                    + " , Zero messages : " + messagesZero
                    + ", Zero counters : " + zeroCounters
                    + " , Total number of counters with prob=1 : " + counterProb
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages);
        }
        return messages;
    }
    public static long collectMessagesMod(String file) throws IOException {

        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long overheadMessages = 0; // overhead of messages to Coordinator
        int zeroCountersCord = 0; // Counter with zero value at Coordinator
        int counterProb = 0; // The total number of counters with probability=1
        // For coordinator file the numMessages = num_round of each Counter - RANDOMIZED case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split(":");
                String sent = splits[splits.length-2].substring(0,splits[splits.length-2].lastIndexOf("o")).trim();

                numMessagesSent = Long.parseLong(sent);
                overheadMessages = Long.parseLong(splits[splits.length-1].trim());
            }

            // if(line.trim().equals("Processing BN queries : 1000")) break;

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("Coordinator state") || line.contains("Worker state ")){
                find = true;
                line = br.readLine();
                continue;
            }

            if(find && line.contains("Counter")){

                // Process the counter

                // Split the lines based on colon
                Counter counter = strToCounter(line);

                if(counter.getNumMessages() == 0L) messagesZero++;
                if(counter.getCounter() == 0L) zeroCounters++;
                if(counter.getLastSentValues() != null) {
                    int zero = 0;
                    for (Long value : counter.getLastSentValues().values()) { if (value == 0L) zero++; }
                    if(zero == counter.getLastSentValues().size()) zeroCountersCord++;
                }
                if(counter.getProb() == 1d) counterProb++;

                // Update the messages
                messages += counter.getNumMessages();

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            System.out.println( "Total counters : " + totalCount
                    + " , Total number of rounds for all counters : " +messages
                    + " , Total number of counters with zero rounds : " + messagesZero
                    + " , Zero counters : " + zeroCountersCord
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages
                    + " , Total number of counters with prob=1 : " + counterProb);
        }
        // Workers
        else {

            System.out.println("Total counters : " + totalCount
                    + " , Total messages : " + messages
                    + " , Zero messages : " + messagesZero
                    + ", Zero counters : " + zeroCounters
                    + " , Total number of counters with prob=1 : " + counterProb
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages);
        }
        return messages;
    }

    // Read file and collect statistics and the number of messages for DC
    public static long collectMessagesDC(String file) throws IOException {

        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long overheadMessages = 0; // overhead of messages to Coordinator
        long numberRounds = 0; // Number of total rounds from all counters
        int zeroRounds = 0; // Counter with zero rounds at Coordinator
        // For coordinator file the counterDoubles = num_round of each Counter - DETERMINISTIC case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split(":");
                String sent = splits[splits.length-2].substring(0,splits[splits.length-2].lastIndexOf("o")).trim();

                numMessagesSent = Long.parseLong(sent);
                overheadMessages = Long.parseLong(splits[splits.length-1].trim());
            }

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("Coordinator state") || line.contains("Worker state ")){
                find = true;
                line = br.readLine();
                continue;
            }

            if(find && line.contains("Counter")){

                // Process the counter

                // Split the lines based on colon
                String[] splitsLine = line.split(",");
                String message = null;

                for (String split : splitsLine) {
                    if (split.contains("numMessages")){
                        message = split.split(":")[1].trim();
                        if(Long.parseLong(message) == 0) messagesZero++;
                    }
                    if(split.contains("counter ")){
                        if(Integer.parseInt(split.split(":")[1].trim()) == 0) zeroCounters += 1;
                    }
                    if(split.contains("counterDoubles")){
                        long numRounds = Long.parseLong(split.split(":")[1].trim());
                        numberRounds += numRounds;
                        if( numRounds == 0) zeroRounds++;
                    }
                }

                // Update the messages
                messages += Long.parseLong(Objects.requireNonNull(message));

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            System.out.println( "Total counters : " + totalCount
                    + " , Total number of rounds for all counters : " + numberRounds
                    + " , Total number of counters with zero rounds : " + zeroRounds
                    + " , Zero counters : " + zeroCounters
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages);
        }
        // Workers
        else {

            System.out.println("Total counters : " + totalCount
                                + " , Total messages : " + messages
                                + " , Zero messages : " + messagesZero
                                + ", Zero counters : " + zeroCounters
                                + " , numMessagesSent : " + numMessagesSent
                                + " , overheadMessages : " + overheadMessages);
        }
        return messages;
    }
    public static long collectMessagesDCMod(String file) throws IOException {

        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long overheadMessages = 0; // overhead of messages to Coordinator
        long numberRounds = 0; // Number of total rounds from all counters
        int zeroRounds = 0; // Counter with zero rounds at Coordinator
        // For coordinator file the counterDoubles = num_round of each Counter - DETERMINISTIC case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split(":");
                String sent = splits[splits.length-2].substring(0,splits[splits.length-2].lastIndexOf("o")).trim();

                numMessagesSent = Long.parseLong(sent);
                overheadMessages = Long.parseLong(splits[splits.length-1].trim());
            }

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("Coordinator state") || line.contains("Worker state ")){
                find = true;
                line = br.readLine();
                continue;
            }

            if(find && line.contains("Counter")){

                // Process the counter

                // Split the lines based on colon
                Counter counter = strToCounter(line);
                if(counter.getNumMessages() == 0L) messagesZero++;
                if(counter.getCounter() == 0L) zeroCounters++;
                numberRounds += counter.getCounterDoubles();
                if(counter.getCounterDoubles() == 0L) zeroRounds++;

                // Update the messages
                messages += counter.getNumMessages();

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            System.out.println( "Total counters : " + totalCount
                    + " , Total number of rounds for all counters : " + numberRounds
                    + " , Total number of counters with zero rounds : " + zeroRounds
                    + " , Zero counters : " + zeroCounters
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages);
        }
        // Workers
        else {

            System.out.println("Total counters : " + totalCount
                    + " , Total messages : " + messages
                    + " , Zero messages : " + messagesZero
                    + ", Zero counters : " + zeroCounters
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages);
        }
        return messages;
    }

    // Collect the messages from workers and coordinator
    public static void collectMessages(String inputFile,String bayesianNetwork,String type,int parallelism,String typeCounter) throws IOException {

        long totalMessagesWorker = 0L;
        long totalMessagesCoordinator = 0L;

        // Collect number of messages for all workers
        for(int i=0;i<parallelism;i++){
            System.out.println("File : " + "worker" + i + ".messages");
            if( typeCounter.equals("RC") ){
                totalMessagesWorker += collectMessagesMod(inputFile+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\link\\Extensions\\RC_DC\\Dummy_Father\\50K\\Experiment_1\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\Desktop\\test\\results-HEPAR2_par=12_slots=1\\RC\\500K\\UNIFORM\\workers = 10_par=10\\result\\"+"worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Communication cost-Number of sites_cluster\\EXACT\\500K\\"+type+"\\workers = 4"+"\\result\\worker"+i+".messages");
                // C:\Users\Nikos\Desktop\test\results-HEPAR2_par=12_slots=1\RC\500K\UNIFORM\workers = 2_par=2\result
                // collectMessages("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Communication cost-Number of sites\\DC\\500K\\"+type+"\\parallelism = 10"+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\500K\\Experiment_1\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5K\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-FGM\\5M\\Experiment_1\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5M\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5M\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE\\5M\\Experient_1\\"+type+"\\worker"+i+".messages");
                // collectMessages("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\worker"+i+".messages");
            }
            else{
                totalMessagesWorker += collectMessagesDC(inputFile+"\\worker"+i+".messages");
            }

        }

        // Collect number of messages for coordinator
        System.out.println("File : coordinator_messages");
        if( typeCounter.equals("RC") ){
            totalMessagesCoordinator += collectMessagesMod(inputFile+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\Desktop\\test\\results-HEPAR2_par=12_slots=1\\RC\\500K\\UNIFORM\\workers = 10_par=10\\result\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Communication cost-Number of sites_cluster\\EXACT\\500K\\"+type+"\\workers = 4"+"\\result\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Communication cost-Number of sites\\DC\\500K\\"+type+"\\parallelism = 10"+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\500K\\Experiment_1\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5K\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-FGM\\5M\\Experiment_1\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5M\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\5M\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE\\5M\\Experient_1\\"+type+"\\coordinator_messages");
            // collectMessages("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\coordinator_messages");
        }
        else{
            totalMessagesCoordinator += collectMessagesDC(inputFile+"\\coordinator_messages");
        }


        System.out.println("Total messages workers : " + totalMessagesWorker);
        System.out.println("Total messages coordinator : " + totalMessagesCoordinator);
    }

    // Collect messages from FGM method
    public static void collectMessagesFGM(String inputFile) throws IOException {

        System.out.println("File : coordinator_messages");
        BufferedReader br = new BufferedReader(new FileReader(inputFile+"\\coordinator_stats.txt"));
        String line = br.readLine();

        long totalMessagesWorkers = 0L; // Total number of messages from Workers
        long numSentMes = 0L; // Number of sent messages
        long numDriftMes = 0L; // Number of drift messages
        long numSenEstMes = 0L; // Number of estimation messages

        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            // Process logic
            if(line.contains("Coordinator state =>")) {

                // Split the lines based on =>
                String splittedLine = line.split("=>")[1];
                System.out.println(splittedLine.trim());

                String[] splits = splittedLine.split(",");

                for (String split : splits) {

                    if (split.contains("numMessagesSent")){
                        numSentMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfSentEstMessages")){
                        numSenEstMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfDriftMessages")){
                        numDriftMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfIncrementMessages")){
                        totalMessagesWorkers += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfZetaMessages")){
                        totalMessagesWorkers += Long.parseLong(split.split(":")[1].trim());
                    }
                }
            }

            // Read the next line
            line = br.readLine();

        }


        System.out.println("Total messages workers : " + totalMessagesWorkers
                + " , numMessagesSent : " + (numSentMes-numSenEstMes) +"\n"
                + "Messages of drift-estimate :  " + numSenEstMes + " + " + numDriftMes + " = "+(numSenEstMes+numDriftMes));
    }

    // Get the runtime
    public static void getRuntime(String inputFile,int parallelism,String type) throws IOException {

        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\DC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\coordinator_stats.txt"));
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\Desktop\\test\\result\\coordinator_stats.txt"));
        BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\coordinator_stats.txt"));
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Throughput_Runtime-Parallelism\\DC\\500K\\"+type+"\\parallelism = "+parallelism+"\\result\\coordinator_stats.txt"));
        String line;
        String firstTs = "";
        String lastTs = "";

        line = br1.readLine();
        while ( line != null ) {

            line = line.replaceAll("\0", "");
            if (!line.contains("received")) { line = br1.readLine(); }

            // Get the first and last timestamp
            if (line != null && line.contains("received first")) {

                firstTs = line;

                // Read the next line
                line = br1.readLine();
            }
            if (line != null && line.contains("received last")) {

                lastTs = line;

                // Read the next line
                line = br1.readLine();
            }


        }

        // Get the runtime
        String[] splitsFirstLine = firstTs.split(",");
        String[] splitsLastLine = lastTs.split(",");

        long first = Long.parseLong(splitsFirstLine[1].trim());
        long last = Long.parseLong(splitsLastLine[1].trim());
        System.out.println("Runtime : "+(last-first)/1000d+" sec");

        br1.close();
    }

    // Calculate the throughput
    public static void getThroughput(String inputFile,int parallelism,String type) throws IOException{

        List<Double> finalThroughput = new ArrayList<>();

        // For all workers
        for(int j=0;j<parallelism;j++) {

            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\DC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\Desktop\\test\\result"+"\\worker_stats_" + j + ".txt"));
            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\RC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\"+"worker_stats_" + j + ".txt"));
            String line;
            List<String> events = new ArrayList<>();
            List<Double> throughputs = new ArrayList<>();

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("event")) { line = br1.readLine(); }

                // Get and add the event
                if (line != null && line.contains("event")) {

                    // Add the event
                    events.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            double throughput = 0d;
            long firstTimestamp = 0L;
            long firstCount = 0L;
            long secondTimestamp = 0L;
            long secondCount = 0L;
            events.remove(0);

            for (int i = 0; i <= events.size(); i++) {

                // Calculate the throughput
                if (i < (events.size() - 1)) {

                    // First row
                    String[] splitsFirstLine = events.get(i).split(",");
                    firstTimestamp = Long.parseLong(splitsFirstLine[1].trim());
                    firstCount = Long.parseLong(splitsFirstLine[2].split(" ")[3].trim());

                    // Second row
                    String[] splitsSecondLine = events.get(i + 1).split(",");
                    secondTimestamp = Long.parseLong(splitsSecondLine[1].trim());
                    secondCount = Long.parseLong(splitsSecondLine[2].split(" ")[3].trim());

                    // Throughput
                    if(secondTimestamp == firstTimestamp) continue;
                    throughput = (secondCount - firstCount) / ((secondTimestamp - firstTimestamp) / 1000d);
                    throughputs.add(throughput);
                    // System.out.println("Throughput : "+throughput);

                }
            }

            // First timestamp
            String[] splitsFirstLine = events.get(0).split(",");
            long timestamp = Long.parseLong(splitsFirstLine[1].trim());
            long count = Long.parseLong(splitsFirstLine[2].split(" ")[3].trim());

            // Last timestamp
            String[] splitsLastLine = events.get(events.size() - 1).split(",");
            long lastTimestamp = Long.parseLong(splitsLastLine[1].trim());
            long lastCount = Long.parseLong(splitsLastLine[2].split(" ")[3].trim());

            throughput = (lastCount - count) / ((lastTimestamp - timestamp) / 1000d);
            // System.out.println("Last Throughput : "+throughput);
            throughputs.add(throughput);

            // Statistics
            // System.out.println("Worker "+j+" => Throughputs : " + throughputs);
            System.out.println("w"+j+" = " + throughputs);
            System.out.println("Worker "+j+" => Count : " + throughputs.size());
            System.out.println("Worker "+j+" => Min Throughput : " + throughputs.stream().min(Double::compareTo).get());
            System.out.println("Worker "+j+" => Max Throughput : " + throughputs.stream().max(Double::compareTo).get());

            throughput = throughputs.stream().mapToDouble(a -> a).average().getAsDouble();
            System.out.println("Worker "+j+" => Average Throughput : " + throughput+"\n");

            // Update the final throughput of system
            finalThroughput.add(throughput);

            br1.close();
        }

        System.out.println("Final Throughputs : "+finalThroughput);
        System.out.println("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get());
        System.out.println("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get());
        System.out.println("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum());
    }

    // Get the tuples and processing time
    public static void getTuplesProc(String inputFile,int parallelism,String type) throws IOException{

        List<Double> finalThroughput = new ArrayList<>();
        List<Double> avgThroughput = new ArrayList<>();

        // For all workers
        for(int j=0;j<parallelism;j++) {

            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\DC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\Desktop\\test\\result"+"\\worker_stats_" + j + ".txt"));
            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\RC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\"+"worker_stats_" + j + ".txt"));
            String line;
            List<String> timers = new ArrayList<>();
            List<Long> numTuples = new ArrayList<>();
            List<Double> processingTime = new ArrayList<>();
            List<Double> throughputs = new ArrayList<>();
            String firstEvent = null;

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("timer")) { line = br1.readLine(); }
                if(line != null && line.contains("receive first event")) firstEvent = line;

                // Get and add the timer
                if (line != null && line.contains("timer")) {

                    // Add the timer
                    timers.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            double throughput = 0d;
            long firstTimestamp = 0L;
            timers.remove(0);

            // Initialize
            numTuples.add(0L);
            processingTime.add(0d);
            throughputs.add(0d);
            firstTimestamp = Long.parseLong(firstEvent.split(" , ")[1].trim());

            for (int i=0;i<timers.size();i++) {

                // Split the line
                String[] splits = timers.get(i).split(" , ");

                // Get the processing time
                processingTime.add((Long.parseLong(splits[1].trim()) - firstTimestamp) / 1000d);

                // Get the number of tuples
                numTuples.add(Long.parseLong(splits[2].split(" ")[2].trim()));

                // Find the throughput
                if(processingTime.get(i+1) >= 1d){ throughputs.add(numTuples.get(i+1)/processingTime.get(i+1)); }

            }

            // Statistics
            // System.out.println("w"+j+" = " + throughputs);
            System.out.println("Worker "+j+" => NumTuples : " + numTuples);
            System.out.println("Worker "+j+" => Processing Time : " + processingTime);
            System.out.println("Worker "+j+" => Throughput : " + throughputs);
            System.out.println("Worker "+j+" => Count proc : " + processingTime.size());
            System.out.println("Worker "+j+" => Count throughtputs : " + throughputs.size());
            System.out.println("Worker "+j+" => Count num : " + numTuples.size());
            System.out.println("Worker "+j+" => Min number of tuples : " + numTuples.stream().min(Long::compareTo).get());
            System.out.println("Worker "+j+" => Max number of tuples : " + numTuples.stream().max(Long::compareTo).get());

            // throughput = numTuples.stream().mapToDouble(a -> a).average().getAsDouble();
            throughput = numTuples.get(numTuples.size()-1)/processingTime.get(processingTime.size()-1);
            avgThroughput.add(throughput);
            System.out.println("Worker "+j+" => Average Throughput : " + throughput);
            throughput = throughputs.stream().mapToDouble(a -> a).sum()/throughputs.size();
            System.out.println("Worker "+j+" => Average Throughput : " +throughput+"\n");

            // Update the final throughput of system
            finalThroughput.add(throughput);

            // Close the buffer
            br1.close();
        }

        System.out.println("Final Throughputs : "+finalThroughput);
        System.out.println("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get());
        System.out.println("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get());
        System.out.println("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum());
        System.out.println("Avg throughput : "+avgThroughput.stream().mapToDouble(a -> a).sum());
    }

    // Calculate the tuples vs processing time for each worker
    public static void getTuples(String inputFile,int parallelism,String type) throws IOException{

        // For all workers
        for(int j=0;j<parallelism;j++) {

            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\DC\\500K\\"+type+"\\workers = "+parallelism+"\\result\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\Desktop\\test\\result"+"\\worker_stats_" + j + ".txt"));
            // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\"+"worker_stats_" + j + ".txt"));
            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));
            String line;
            List<String> events = new ArrayList<>();
            List<Long> tuples = new ArrayList<>();
            List<Double> time = new ArrayList<>();

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("event")) { line = br1.readLine(); }

                // Get and add the event
                if (line != null && line.contains("event")) {

                    // Add the event
                    events.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            long firstTimestamp;
            events.remove(0);

            // Get the first timestamp
            String[] splitsFirstLine = events.get(0).split(",");
            firstTimestamp = Long.parseLong(splitsFirstLine[1].trim());
            time.add(0d);tuples.add(0L);

            for (int i = 1; i < events.size(); i++) {

                String[] splits = events.get(i).split(",");

                // Get total count,add to list
                tuples.add(Long.parseLong(splits[2].split(" ")[3].trim()));

                // Get the processing time,add to list
                time.add(((Long.parseLong(splits[1].trim()) - firstTimestamp)/1000d));
            }

            // Statistics
            System.out.println("Worker "+j+" => Tuples : " + tuples);
            System.out.println("Worker "+j+" => Processing time : " + time);
            System.out.println("Size : "+tuples.size()+"(tuples)"+","+time.size());

            br1.close();
        }
    }

    // Check the ordering of data
    public static void testOrdering(int parallelism) throws IOException{

        // For all workers
        for(int j=0;j<parallelism;j++) {

            BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\res1\\"+"worker_stats_" + j + ".txt"));
            BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\"+"worker_stats_" + j + ".txt"));
            String line;
            List<String> input1 = new ArrayList<>();
            List<String> input2 = new ArrayList<>();

            // Get the input from the first file
            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("input")) { line = br1.readLine(); }

                // Get and add the timer
                if (line != null && line.contains("input")) {

                    // Add the timer
                    input1.add(line.split(" : ")[1].trim());

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Get the input from the second file
            line = br2.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("input")) { line = br2.readLine(); }

                // Get and add the timer
                if (line != null && line.contains("input")) {

                    // Add the timer
                    input2.add(line.split(" : ")[1].trim());

                    // Read the next line
                    line = br2.readLine();
                }
            }

            // Check the ordering
            System.out.println("Worker "+j);
            System.out.println("Input1 size : "+input1.size());
            System.out.println("Input2 size : "+input2.size());
            for(int i=0;i<input1.size();i++){
                if(!input1.get(i).equals(input2.get(i))){
                    System.out.println("Ordering violation, "+i);
                    System.out.println("Input1 value : "+input1.get(i));
                    System.out.println("Input2 value : "+input2.get(i));
                }
            }

            // Close the buffers
            br1.close();br2.close();
        }

    }

    // Read file line by line
    public static void readFile() throws IOException{

        // Read the file line by line and send to topic topicName
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\datasets\\data_link0_5000000")); // input file as args[0]
        String line = br.readLine();

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\data_link0_5000000",true));

        // Total count
        long totalCount = 0L;

        while (line != null) {

            // Strips off all non-ASCII characters
            line = line.replaceAll("[^\\x00-\\x7F]", "");
            // Erases all the ASCII control characters
            line = line.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
            // Skip empty lines
            if (line.equals("")) {
                line = br.readLine();
                continue;
            }
            // Skip the schema of dataset
            if (line.equals(new BufferedReader(new FileReader("src\\main\\java\\bayesianNetworks\\dataset_schema_link")).readLine())) {
                System.out.println("Found dataset schema!!!");
                line = br.readLine();
                continue;
            }
            // EOF
            if(line.equals("EOF")) System.out.println("EOF!!!");

            // Print the line
            System.out.println(line);

            // Write the line
            writer.write(line+"\n");

            // Read the next line
            line = br.readLine();

            // Update the counter
            totalCount += 1;
        }

        System.out.println("Total count : "+totalCount);
        // writer.write("EOF\n");

        // Close buffers
        br.close();
        writer.close();

    }




    /** Calculate the next power of 2 that is smaller or equal than x
     * where x = eps*lastBroadcastValue / sqrt(numWorkers)*resConstant
     * @param eps Epsilon
     * @param lastBroadcastValue Last broadcast value from coordinator
     * @param numWorkers Number of workers
     * @param resConstant Rescaling constant of counter
     * @return The highest power of 2 that is smaller than x
     */
    public static double highestPowerOf2(double eps,long lastBroadcastValue,int numWorkers,double resConstant){

        if( numWorkers == 0 || resConstant == 0 ) return 1;

        double numerator = eps * lastBroadcastValue;
        double denominator = Math.sqrt(numWorkers) * resConstant;

        if(numerator == 0) return 1;

        return (Math.log(numerator/denominator) / Math.log(2));
    }

    public static double calculateCondition(double eps,long condition,int numWorkers){

        if( numWorkers == 0) return 0;
        return  (eps*condition)/numWorkers;
    }

    // Compare two files line by line
    // compareFiles("querySource","querySourceStatistics")
    public static void compareFiles(String file,String file1) throws IOException {


        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));

        // Second input
        BufferedReader br1 = new BufferedReader(new FileReader(file1));

        String line = br.readLine();
        String line1 = br1.readLine();

        int totalCount = 0;
        while ( line != null && line1 != null ){

            if(!line.replaceAll("\0","").equals(line1.replaceAll("\0",""))){
                System.out.println("Line : "+(totalCount+1)+" not equal");
                return;
            }

            // Update the total count
            totalCount++;

            // Read the next line
            line = br.readLine();
            line1 = br1.readLine();

        }

        System.out.println("Equal lines : "+totalCount);

    }

    // Replace new lines
    public static void replaceNewLines(String file) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\worker_stats_2.txt")); // input file as args[0]
        BufferedWriter writer = new BufferedWriter(new FileWriter("worker_stats_2_mod.txt"));

        String line = br.readLine();
        while ( line != null ){

            line = line.replaceAll("\0","").trim();

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Write the line
            writer.write("\n"+line+"\n");

            // Read the next line
            line = br.readLine();

        }

        // Closing
        br.close();
        writer.close();
    }

    // Compare error setup schemas
    public static void compareSchemas(int parallelism) throws IOException {

        long totalDiffMessages = 0L;

        // For each worker
        for(int i=0;i<parallelism;i++) {

            System.out.println("\n\nFile : " + "worker" + i + ".messages");

            // First input,first schema
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-RC\\50M\\Experiment_1\\BASELINE" + "\\worker" + i + ".messages"));
            String line = br.readLine();
            boolean find = false;
            Vector<Counter> counters1 = new Vector<>();

            // First input
            while (line != null) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                // Skip the lines , until reach the Coordinator or Worker state
                if (line.contains("Coordinator state") || line.contains("Worker state ")) {
                    find = true;
                    line = br.readLine();
                    continue;
                }

                if (find && line.contains("Counter")) {

                    // Get the counter
                    Counter counter = strToCounter(line);
                    counters1.add(counter);
                }

                // Read the next line
                line = br.readLine();

            }

            // Initialize the first hash map
            LinkedHashMap<Long, Counter> newCounters1 = new LinkedHashMap<>();
            for (Counter counter : counters1) {

                // Get the key
                String key = getKey(counter);

                // Hash the key
                long hashedKey = hashKey(key.getBytes());

                // Update the worker state
                newCounters1.put(hashedKey, counter);

                // Update the counter
                counter.setCounterKey(hashedKey);
            }

            BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Error_to_MLE-RC\\50M\\Experiment_1\\UNIFORM" + "\\worker" + i + ".messages"));
            line = br2.readLine();
            find = false;
            Vector<Counter> counters2 = new Vector<>();

            // Second Input
            while (line != null) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br2.readLine();
                    continue;
                }

                // Skip the lines , until reach the Coordinator or Worker state
                if (line.contains("Coordinator state") || line.contains("Worker state ")) {
                    find = true;
                    line = br2.readLine();
                    continue;
                }

                if (find && line.contains("Counter")) {

                    // Get the counter
                    Counter counter = strToCounter(line);
                    counters2.add(counter);
                }

                // Read the next line
                line = br2.readLine();

            }

            // Initialize the first hash map
            LinkedHashMap<Long, Counter> newCounters2 = new LinkedHashMap<>();
            for (Counter counter : counters2) {

                // Get the key
                String key = getKey(counter);

                // Hash the key
                long hashedKey = hashKey(key.getBytes());

                // Update the worker state
                newCounters2.put(hashedKey, counter);

                // Update the counter
                counter.setCounterKey(hashedKey);
            }

            // Compare the counters of each schema
            System.out.println("Counters1 size : " + newCounters1.size());
            System.out.println("Counters2 size : " + newCounters2.size());

            // Messages
            long sumDiffMess = 0L;long diffMess;
            long lessMess = 0;long biggerMess = 0;long equalMess = 0;long zeroMess1 = 0;long zeroMess2 = 0;

            // Epsilon
            double diffEps;double sumDiffEps = 0d;
            long lessEps = 0;long biggerEps = 0;long equalEps = 0;long zeroEps1 = 0;long zeroEps2 = 0;

            // Probabilities
            double diffProb;double sumDiffProb = 0d;
            long lessProb = 0;long biggerProb = 0;long equalProb = 0;long zeroProb1 = 0;long zeroProb2 = 0;

            // Counter of equals counters
            long equalCounters = 0L;

            // Zero counter
            long zeroCounters1 = 0;long zeroCounters2 = 0;

            // For each counter
            for (Counter counter : newCounters1.values()) {

                // Get the second counter
                Counter counter2 = newCounters2.get(counter.getCounterKey());

                // Check the counter
                if (counter.getNodeName().equals(counter2.getNodeName())
                        && counter.getNodeValue().equals(counter2.getNodeValue())
                        && counter.isActualNode() == counter2.isActualNode()
                        && counter.getTrueParameter() == counter2.getTrueParameter()
                        && counter.getCounter() == counter2.getCounter()
                        && counter.getCounterDoubles() == counter2.getCounterDoubles()) {

                    if (counter.getNodeParents() != null) {
                        if (!counter.getNodeParents().equals(counter2.getNodeParents()))
                            System.out.println("Not equal counters");
                    }
                    if (counter.getNodeParentValues() != null) {
                        if (!counter.getNodeParentValues().equals(counter2.getNodeParentValues()))
                            System.out.println("Not equal counters");
                    }

                    // System.out.println("\nCounter_value : " + (counter.getNodeParentValues() == null ? counter.getNodeValue() : counter.getNodeValue().toString().concat(counter.getNodeParentValues().toString())));
                    // System.out.println("1 => numMessages : " + counter.getNumMessages() + " , prob : " + counter.getProb() + " , epsilon : " + counter.getEpsilon());
                    // System.out.println("2 => numMessages : " + counter2.getNumMessages() + " , prob : " + counter2.getProb() + " , epsilon : " + counter2.getEpsilon());

                    // Difference on messages
                    diffMess = counter.getNumMessages() - counter2.getNumMessages();
                    sumDiffMess += diffMess;

                    if (counter.getNumMessages() > counter2.getNumMessages()) biggerMess++;
                    else if (counter.getNumMessages() < counter2.getNumMessages()) lessMess++;
                    else equalMess++;

                    // Zero messages
                    if (counter.getNumMessages() == 0d) zeroMess1++;
                    if (counter2.getNumMessages() == 0d) zeroMess2++;


                    // Difference on epsilon
                    diffEps = Math.abs(counter.getEpsilon() - counter2.getEpsilon());
                    sumDiffEps += diffEps;
                    if (counter.getEpsilon() > counter2.getEpsilon()) biggerEps++;
                    else if (counter.getEpsilon() < counter2.getEpsilon()) lessEps++;
                    else equalEps++;
                    if(counter.getEpsilon() == 0d) zeroEps1++;
                    if(counter2.getEpsilon() == 0d) zeroEps2++;

                    // Difference on probabilities
                    if (counter.getProb() > counter2.getProb()) biggerProb++;
                    else if (counter.getProb() < counter2.getProb()) lessProb++;
                    else equalProb++;

                    // Zero probabilities
                    if (counter.getProb() == 1d) zeroProb1++;
                    if (counter2.getProb() == 1d) zeroProb2++;

                    diffProb = counter.getProb() - counter2.getProb();
                    sumDiffProb += diffProb;

                    // Zero counters
                    if(counter.getCounter() == 0L) zeroCounters1++;
                    if(counter2.getCounter() == 0L) zeroCounters2++;

                    // System.out.println("Diff => numMessages : " + diffMess + " , prob : " + diffProb + " , epsilon : " + diffEps);
                    equalCounters++;
                }
            }

            System.out.println("Equal counters : " + equalCounters);
            System.out.println("Zero counters1 : " + zeroCounters1);
            System.out.println("Zero counters2 " + zeroCounters2);

            System.out.println("Counters1 with zero messages : " + zeroMess1);
            System.out.println("Counters2 with zero messages : " + zeroMess2);
            System.out.println("Equal messages : " + equalMess + " , less messages : " + lessMess + " , bigger messages : " + biggerMess);
            System.out.println("Difference on messages : " + sumDiffMess);

            System.out.println("Counters1 zero epsilon : " + zeroEps1);
            System.out.println("Counters2 zero epsilon " + zeroEps2);
            System.out.println("Equal epsilon : " + equalEps + " , less epsilon : " + lessEps + " , bigger epsilon : " + biggerEps);
            System.out.println("Average difference on epsilon : " + (sumDiffEps / counters1.size()));

            System.out.println("Counters1 with one probability : " + zeroProb1);
            System.out.println("Counters2 with one probability : " + zeroProb2);
            System.out.println("Equal probabilities : " + equalProb + " , less probabilities : " + lessProb + " , bigger probabilities : " + biggerProb);
            System.out.println("Average difference on probability : " + (sumDiffProb / counters1.size()));

            // Update total difference on messages
            totalDiffMessages += sumDiffMess;

        }

        System.out.println("\nTotal difference on messages : " + totalDiffMessages);
    }

    // Read file line by line and discard the end of the line
    public static void discardEnd() throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter("discardedQueries"));

        BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries1"));
        String line1 = br1.readLine();

        int querySize = 0;

        while ( line1 != null ) {

            line1 = line1.replaceAll("\0", "");

            if( !line1.contains("Input") ){ line1 = br1.readLine(); }
            if(  line1 != null && line1.contains("Input") ) {

                // Discard the end and write to the file
                String[] splitsLine1 = line1.split(", estimated probability");
                writer.write(splitsLine1[0]+"\n");

                // Read the next lines
                line1 = br1.readLine();
                querySize++;
            }

        }

        System.out.println("Queries size : " + querySize);

        // Close buffers
        br1.close();
        writer.close();

    }

    // Compare two files
    public static void compare(String network,String sizeOfDataset,String type) throws IOException {

        BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+network+"\\Extensions\\RC_DC\\Dummy_Father\\"+type+"\\"+sizeOfDataset+"\\queries1"));
        BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+network+"\\Extensions\\RC_DC\\Dummy_Father\\"+type+"\\"+sizeOfDataset+"\\queries2"));

        String line1;
        int querySize = 0;
        int counter = 0;
        double error = 0d;
        double avg_abs_error = 0d;
        int lbv = 0,ubv = 0;

        String errors = "";
        String errors_runs = "";
        String errors_runs_200 = "";
        double tmp_error = 0;

        String line2 = br2.readLine();
        while ( line2 != null ) {

            line2 = line2.replaceAll("\0","");
            if( !line2.contains("Input") ){ line2 = br2.readLine(); }

            if(  line2 != null && line2.contains("Input") ){

                String[] splitsLine2 = line2.split(":");
                String value2 = null; String estProb2 = null;

                // For the second line get the value and probability
                for(int i=0;i<splitsLine2.length;i++){
                    if(splitsLine2[i].contains("value")) value2 = splitsLine2[i+1];
                    if(splitsLine2[i].contains("estimated probability")) estProb2 = splitsLine2[i+1];
                }

                // First line
                String[] splitsLine1;
                String value1 = null; String estProb1 = null;

                // Search for the line2
                br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+network+"\\Extensions\\RC_DC\\Dummy_Father\\"+type+"\\"+sizeOfDataset+"\\queries1"));
                line1 = br1.readLine();
                while ( line1 != null ){

                    line1 = line1.replaceAll("\0","");
                    if( !line1.contains("Input") ){ line1 = br1.readLine(); }
                    if(  line1 != null && line1.contains("Input") ){

                        // Split the line
                        splitsLine1 = line1.split(":");

                        // For the first line get the value and probability
                        for(int i=0;i<splitsLine1.length;i++){
                            if(splitsLine1[i].contains("value")) value1 = splitsLine1[i+1];
                            if(splitsLine1[i].contains("estimated probability")) estProb1 =  splitsLine1[i+1];
                        }

                        // Check the value
                        value1 = value1.trim(); value2 = value2.trim();
                        if(!value1.equals(value2)){
                            counter++;
                            line1 = br1.readLine();
                        }
                        else { break; }
                    }
                }

                if(counter == 1000){
                    System.out.println("Count : "+querySize+" , queries are not equal");
                    return;
                }
                else{ counter = 0; }

                // Find the error between two lines(estimated-exact)
                double estProb = Double.parseDouble(estProb1.split(",")[0]);
                double exactProb = Double.parseDouble(estProb2.split(",")[0]);
                double errorLine = estProb - exactProb;
                System.out.println((querySize+1)+" )" + " -0.1 <= ln(estimated) - ln(truth) = " + (estProb-exactProb) + " <= 0.1" );

                // Check the bounds
                // Up to two decimal places precession : Precision.equals(0.104d,0.1d,0.01);
                if( (estProb-exactProb) >= 0.1d ){ ubv++; }
                else if( (estProb-exactProb) <= -0.1d ){ lbv++; }

                // Append the error
                errors = errors.concat(errorLine +",");

                // Update the error
                error += errorLine;
                avg_abs_error += Math.abs(errorLine);

                // Update
                querySize++;

                // Append the avg error per 200 queries and calculate the avg error per 200 queries
                tmp_error += errorLine;
                if( querySize != 0 && querySize % 200 == 0 ){
                    tmp_error = tmp_error/200;
                    errors_runs = errors_runs.concat(tmp_error+",");
                    errors_runs_200 = errors_runs_200.concat((error / querySize) +",");
                    tmp_error = 0;
                }

                // Read the next lines
                line2 = br2.readLine();
            }

        }


        System.out.println("Queries2 size : " + querySize);
        System.out.println("Error to EXACTMLE");
        System.out.println( "Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv);
        System.out.println( "\nAverage absolute error : " + avg_abs_error/querySize);
        System.out.println( "Errors : " + errors.substring(0,errors.length()-1));
        System.out.println( "Errors runs : " + errors_runs.substring(0,errors_runs.length()-1));
        System.out.println( "Errors runs 200 : " + errors_runs_200.substring(0,errors_runs_200.length()-1));

        // Close buffers
        br1.close();
        br2.close();
    }

    // Compare the workers for different value of parameter workers
    public static void checkWorkers(String file1,int workers1,String file2,int workers2,String outputFile) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"stats"));

        // Counters from the first file
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }



        // Counters from the second file
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(strToCounter(line));
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }



        // Compare the counters
        System.out.println("Counters1 size : "+counters1.size());
        System.out.println("Counters2 size : "+counters2.size());

        long equalProb=0;long lessProb=0;long biggerProb=0;long prob1=0;
        long equalMes=0;long lessMes=0;long biggerMes=0;
        long equalBroad=0;long lessBroad=0;long biggerBroad=0;
        long equalRound=0;long lessRound=0;long biggerRound=0;
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Check epsilon,delta,resConstant,trueParameter
            if( (counters1.get(i).getEpsilon() != counters2.get(i).getEpsilon()) &&
                (counters1.get(i).getDelta() != counters2.get(i).getDelta()) &&
                (counters1.get(i).getResConstant() != counters2.get(i).getResConstant()) &&
                (counters1.get(i).getTrueParameter() != counters2.get(i).getTrueParameter()) ){
                System.out.println("Counters not equal");
                continue;
            }

            // Probabilities
            if(counters1.get(i).getProb() == 1d && counters2.get(i).getProb() == 1d) prob1++;
            else if(counters1.get(i).getProb() == counters2.get(i).getProb()) equalProb++;
            else if(counters1.get(i).getProb() > counters2.get(i).getProb()) biggerProb++;
            else lessProb++;

            // Rounds
            if(counters1.get(i).getNumMessages() == counters2.get(i).getNumMessages()) equalRound++;
            else if(counters1.get(i).getNumMessages() > counters2.get(i).getNumMessages()) biggerRound++;
            else lessRound++;

            // Broadcast value
            if(counters1.get(i).getLastBroadcastValue() == counters2.get(i).getLastBroadcastValue()) equalBroad++;
            else if(counters1.get(i).getLastBroadcastValue() > counters2.get(i).getLastBroadcastValue()) biggerBroad++;
            else lessBroad++;

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                    + " , value : " + counters1.get(i).getNodeValue()
                    + " , actualNode : " + counters1.get(i).isActualNode()
                    + " , nodeParents : " + counters1.get(i).getNodeParents()
                    + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                    + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);
            writer.write(s+"\n");

            // Get the total of number of messages for counter
            long totalNumOfMessages1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessages1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            String s1 = "Counter1 => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                        + " , numOfRounds : "+counters1.get(i).getNumMessages()
                        + " , prob : "+counters1.get(i).getProb()
                        + " , sumOfValues : "+sumOfLastSentValues(counters1.get(i))
                        + " , sumOfUpdates : "+sumOfUpdates(counters1.get(i))
                        + " , estimator : "+calculateEstimator(counters1.get(i))
                        + " , numOfMessages : "+totalNumOfMessages1
                        + " , (sqrt(k)*resConstant)/epsilon : "+(Math.sqrt(workers1)*counters1.get(i).getResConstant())/counters1.get(i).getEpsilon()
                        + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers1)/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters1.get(i)));

            System.out.println(s1);
            writer.write(s1+"\n");

            long totalNumOfMessages2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessages2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            String s2 = "Counter2 => lastBroadcastValue : "+counters2.get(i).getLastBroadcastValue()
                    + " , numOfRounds : "+counters2.get(i).getNumMessages()
                    + " , prob : "+counters2.get(i).getProb()
                    + " , sumOfValues : "+sumOfLastSentValues(counters2.get(i))
                    + " , sumOfUpdates : "+sumOfUpdates(counters2.get(i))
                    + " , estimator : "+calculateEstimator(counters2.get(i))
                    + " , numOfMessages : "+totalNumOfMessages2
                    + " , (sqrt(k)*resConstant)/epsilon : "+(Math.sqrt(workers2)*counters2.get(i).getResConstant())/counters2.get(i).getEpsilon()
                    + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers2)/counters2.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)));

            System.out.println(s2+"\n");
            writer.write(s2+"\n");

            // Messages
            if(totalNumOfMessages1 == totalNumOfMessages2) equalMes++;
            else if(totalNumOfMessages1 > totalNumOfMessages2) biggerMes++;
            else lessMes++;
        }

        System.out.println("Probabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1);
        writer.write("\nProbabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1+"\n");

        System.out.println("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes);
        writer.write("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+"\n");

        System.out.println("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound);
        writer.write("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound+"\n");

        System.out.println("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad);
        writer.write("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad+"\n");

        // Close the buffer
        writer.close();
    }

    // Compare the workers for different value of parameter epsilon
    public static void checkEpsilon(String file1,int workers1,String file2,int workers2,String outputFile) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"stats3"));

        // Counters from the first file
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }


        // Counters from the second file
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(strToCounter(line));
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }



        // Compare the counters
        System.out.println("Counters1 size : "+counters1.size());
        System.out.println("Counters2 size : "+counters2.size());


        long equalProb=0;long lessProb=0;long biggerProb=0;long prob1=0;
        long equalMes=0;long lessMes=0;long biggerMes=0;long zeroMes=0;
        long equalRound=0;long lessRound=0;long biggerRound=0;
        long equalBroad=0;long lessBroad=0;long biggerBroad=0;
        long equalEpsilon=0;long lessEpsilon=0;long biggerEpsilon=0;
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Check epsilon,delta,resConstant,trueParameter
            if( (counters1.get(i).getDelta() != counters2.get(i).getDelta()) &&
                 (counters1.get(i).getResConstant() != counters2.get(i).getResConstant()) &&
                 (counters1.get(i).getTrueParameter() != counters2.get(i).getTrueParameter()) ){
                System.out.println("Counters not equal");
                continue;
            }

            // Epsilon
            if(counters1.get(i).getEpsilon() == counters2.get(i).getEpsilon()) equalEpsilon++;
            else if(counters1.get(i).getEpsilon() > counters2.get(i).getEpsilon()) biggerEpsilon++;
            else lessEpsilon++;

            // Probabilities
            if(counters1.get(i).getProb() == 1d && counters2.get(i).getProb() == 1d) prob1++;
            else if(counters1.get(i).getProb() == counters2.get(i).getProb()) equalProb++;
            else if(counters1.get(i).getProb() > counters2.get(i).getProb()) biggerProb++;
            else lessProb++;

            // Rounds
            if(counters1.get(i).getNumMessages() == counters2.get(i).getNumMessages()) equalRound++;
            else if(counters1.get(i).getNumMessages() > counters2.get(i).getNumMessages()) biggerRound++;
            else lessRound++;

            // Broadcast value
            if(counters1.get(i).getLastBroadcastValue() == counters2.get(i).getLastBroadcastValue()) equalBroad++;
            else if(counters1.get(i).getLastBroadcastValue() > counters2.get(i).getLastBroadcastValue()) biggerBroad++;
            else lessBroad++;

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                    + " , value : " + counters1.get(i).getNodeValue()
                    + " , actualNode : " + counters1.get(i).isActualNode()
                    + " , nodeParents : " + counters1.get(i).getNodeParents()
                    + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                    + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);
            writer.write(s+"\n");

            // Get the total of number of messages for counter
            long totalNumOfMessagesWorkers1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessagesWorkers1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages1= totalNumOfMessagesWorkers1+(counters1.get(i).getNumMessages()*workers1);
            String s1 = "Counter1 => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                    + " , numOfRounds : "+counters1.get(i).getNumMessages()
                    + " , prob : "+counters1.get(i).getProb()
                    + " , sumOfValues : "+sumOfLastSentValues(counters1.get(i))
                    + " , sumOfUpdates : "+sumOfUpdates(counters1.get(i))
                    + " , estimator : "+calculateEstimator(counters1.get(i))
                    + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers1
                    + " , numOfMessagesCoordinator : "+counters1.get(i).getNumMessages()*workers1
                    + " , totalMessages : "+totalNumOfMessages1
                    + " , (sqrt(k)*resConstant)/epsilon : "+(Math.sqrt(workers1)*counters1.get(i).getResConstant())/counters1.get(i).getEpsilon()
                    + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers1)/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters1.get(i)));

            System.out.println(s1);
            writer.write(s1+"\n");

            long totalNumOfMessagesWorkers2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessagesWorkers2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages2= totalNumOfMessagesWorkers2+(counters2.get(i).getNumMessages()*workers2);
            String s2 = "Counter2 => lastBroadcastValue : "+counters2.get(i).getLastBroadcastValue()
                    + " , numOfRounds : "+counters2.get(i).getNumMessages()
                    + " , prob : "+counters2.get(i).getProb()
                    + " , sumOfValues : "+sumOfLastSentValues(counters2.get(i))
                    + " , sumOfUpdates : "+sumOfUpdates(counters2.get(i))
                    + " , estimator : "+calculateEstimator(counters2.get(i))
                    + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers2
                    + " , numOfMessagesCoordinator : "+counters2.get(i).getNumMessages()*workers2
                    + " , totalMessages : "+totalNumOfMessages2
                    + " , (sqrt(k)*resConstant)/epsilon : "+(Math.sqrt(workers2)*counters2.get(i).getResConstant())/counters2.get(i).getEpsilon()
                    + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers2)/counters2.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)));

            System.out.println(s2+"\n");
            writer.write(s2+"\n");

            // Messages
            if(totalNumOfMessages1 == 0 &&  totalNumOfMessages2 == 0) zeroMes++;
            else if(totalNumOfMessages1 == totalNumOfMessages2) equalMes++;
            else if(totalNumOfMessages1 > totalNumOfMessages2) biggerMes++;
            else lessMes++;
        }

        System.out.println("Probabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1);
        writer.write("\nProbabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1+"\n");

        System.out.println("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes);
        writer.write("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes+"\n");

        System.out.println("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound);
        writer.write("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound+"\n");

        System.out.println("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad);
        writer.write("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad+"\n");

        System.out.println("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon);
        writer.write("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon+"\n");

        // Counters1
        System.out.println("\nCounters1");
        for (Counter counter : counters1) {
            for(int i=0;i<workers1;i++){
                if (counter.getProb() != workersCounters1.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        // Counters2
        System.out.println("\nCounters2");
        for (Counter counter : counters2) {
            for(int i=0;i<workers2;i++){
                if (counter.getProb() != workersCounters2.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        // Close the buffer
        writer.close();
    }

    // Check queries for DC and EXACT
    public static void checkQueries(String file1,int workers1,String file2,int workers2,String outputFile) throws IOException {

        // Counters from the first file(Coordinator)
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }


        // Broadcast values and sync
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    System.out.println("Different broadcast value : worker "+j+" => "+counters1.get(i));

                }
            }
        }

        long countBroad = 0L;
        long countSync = 0L;
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    countBroad++;
                    break;
                }
            }
        }

        for(int j=0;j<workers1;j++){

            for(int i=0;i<counters1.size();i++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).isSync() ){
                    countSync++;
                }
            }
            System.out.println("Worker "+j+" , counters sync : "+countSync);
            countSync=0;
        }
        System.out.println("Counters different broadcast value : "+countBroad);



        // Counters from the second file
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(strToCounter(line));
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }




        // Compare counters
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Check epsilon,delta,resConstant,trueParameter
            if( (counters1.get(i).getDelta() != counters2.get(i).getDelta()) &&
                (counters1.get(i).getResConstant() != counters2.get(i).getResConstant()) &&
                (counters1.get(i).getTrueParameter() != counters2.get(i).getTrueParameter()) ){
                System.out.println("Counters not equal");
                continue;
            }

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                        + " , value : " + counters1.get(i).getNodeValue()
                        + " , actualNode : " + counters1.get(i).isActualNode()
                        + " , nodeParents : " + counters1.get(i).getNodeParents()
                        + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                        + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);

            // Messages from file1
            long totalNumOfMessagesWorkers1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessagesWorkers1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages1= totalNumOfMessagesWorkers1+(counters1.get(i).getNumMessages()*workers1);

            // Messages from file2
            long totalNumOfMessagesWorkers2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessagesWorkers2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages2= totalNumOfMessagesWorkers2+(counters2.get(i).getNumMessages()*workers2);

            long estimator = counters1.get(i).getCounter();
            long exactEstimator = calculateEstimator(counters2.get(i));
            long diff = estimator-exactEstimator;

            String s1 = "Counter1 => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                    + " , numOfRounds : "+counters1.get(i).getNumMessages()
                    + " , prob : "+counters1.get(i).getProb()
                    + " , sumOfValues : "+sumOfLastSentValues(counters1.get(i))
                    + " , sumOfUpdates : "+sumOfUpdates(counters1.get(i))
                    + " , estimator : "+calculateEstimator(counters1.get(i))
                    + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers1
                    + " , numOfMessagesCoordinator : "+counters1.get(i).getNumMessages()*workers1
                    + " , totalMessages : "+totalNumOfMessages1
                    + " , (sqrt(k)*resConstant)/epsilon : "+(Math.sqrt(workers1)*counters1.get(i).getResConstant())/counters1.get(i).getEpsilon()
                    + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers1)/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters1.get(i)));

            System.out.println(s1);
        }

        // Get queries
        // List<Input> queries = getQueries("");

        // Compare queries


    }

    // Compare the DC and EXACT type
    public static void checkDC(String file1,int workers1,String file2,int workers2,String file3,int workers3,String outputFile) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"stats4"));

        // Counters from the first file
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Counters from the second file
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();


        // Counters from the third file
        Vector<Counter> counters3 = new Vector<>();
        br = new BufferedReader(new FileReader(file3+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters3.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters3 = new LinkedHashMap<>();
        for(int i=0;i<workers3;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file3+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters3.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers3;i++){
            if(workersCounters3.get(i).size() != counters3.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Compare the counters
        System.out.println("Counters1 size : "+counters1.size());
        System.out.println("Counters2 size : "+counters2.size());
        System.out.println("Counters3 size : "+counters3.size());


        long equalProb=0;long lessProb=0;long biggerProb=0;long prob1=0;
        long equalMes=0;long lessMes=0;long biggerMes=0;long zeroMes=0;
        long equalRound=0;long lessRound=0;long biggerRound=0;
        long equalBroad=0;long lessBroad=0;long biggerBroad=0;
        long equalEpsilon=0;long lessEpsilon=0;long biggerEpsilon=0;
        long upperViolation=0;long lowerViolation=0;
        long countBroad=0;
        long totalMessagesWorkers1=0;long totalMessagesCoord1=0;
        long totalMessagesWorkers2=0;long totalMessagesCoord2=0;
        long totalMessagesWorkers3=0;long totalMessagesCoord3=0;
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()
               && counters1.get(i).getCounterKey() != counters3.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Check delta,resConstant,trueParameter
            if( (counters1.get(i).getDelta() != counters2.get(i).getDelta()) &&
                (counters1.get(i).getResConstant() != counters2.get(i).getResConstant()) &&
                (counters1.get(i).getTrueParameter() != counters2.get(i).getTrueParameter()) ){
                System.out.println("Counters not equal");
                continue;
            }

            // Epsilon
            if(counters1.get(i).getEpsilon() == counters2.get(i).getEpsilon()) equalEpsilon++;
            else if(counters1.get(i).getEpsilon() > counters2.get(i).getEpsilon()) biggerEpsilon++;
            else lessEpsilon++;

            // Probabilities
            if(counters1.get(i).getProb() == 1d && counters2.get(i).getProb() == 1d) prob1++;
            else if(counters1.get(i).getProb() == counters2.get(i).getProb()) equalProb++;
            else if(counters1.get(i).getProb() > counters2.get(i).getProb()) biggerProb++;
            else lessProb++;

            // Rounds
            if(counters1.get(i).getCounterDoubles() == counters2.get(i).getCounterDoubles()) equalRound++;
            else if(counters1.get(i).getCounterDoubles() > counters2.get(i).getCounterDoubles()) biggerRound++;
            else lessRound++;

            // Broadcast value
            if(counters1.get(i).getLastBroadcastValue() == counters2.get(i).getLastBroadcastValue()) equalBroad++;
            else if(counters1.get(i).getLastBroadcastValue() > counters2.get(i).getLastBroadcastValue()) biggerBroad++;
            else lessBroad++;

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                        + " , value : " + counters1.get(i).getNodeValue()
                        + " , actualNode : " + counters1.get(i).isActualNode()
                        + " , nodeParents : " + counters1.get(i).getNodeParents()
                        + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                        + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);
            writer.write(s+"\n");

            // Get the total of number of messages for counter
            long totalNumOfMessagesWorkers1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessagesWorkers1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages1= totalNumOfMessagesWorkers1+(2*counters1.get(i).getCounterDoubles()*workers1);
            long totalNumOfMessagesCoord1 = 2*counters1.get(i).getCounterDoubles()*workers1;
            totalMessagesWorkers1 += totalNumOfMessagesWorkers1;
            totalMessagesCoord1 += totalNumOfMessagesCoord1;
            String s1 = "Counter1 => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                        + " , numOfRounds : "+counters1.get(i).getCounterDoubles()
                        + " , estimator : "+counters1.get(i).getCounter()
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers1
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord1
                        + " , totalMessages : "+totalNumOfMessages1
                        + " , k/epsilon : "+workers1/counters1.get(i).getEpsilon()
                        + " , (k/epsilon)*logN : "+(workers1/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                        + " ,"+((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+counters1.get(i).getCounter()+" <= "+((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))
                        + " , numDriftInc : "+counters1.get(i).getNumMessages();

            System.out.println(s1);
            writer.write(s1+"\n");

            // Violations on counters
            if( counters1.get(i).getCounter() > ((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Upper violation \n");
                upperViolation++;
            }
            else if( counters1.get(i).getCounter() < ((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Lower violation \n");
                lowerViolation++;
            }


            // Get the total of number of messages for counter from third file
            long totalNumOfMessagesWorkers3=0;
            for(int j=0;j<workers3;j++){
                totalNumOfMessagesWorkers3 += workersCounters3.get(j).get(counters3.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages3= totalNumOfMessagesWorkers3+(2*counters3.get(i).getCounterDoubles()*workers3);
            long totalNumOfMessagesCoord3 = 2*counters3.get(i).getCounterDoubles()*workers3;
            totalMessagesWorkers3 += totalNumOfMessagesWorkers3;
            totalMessagesCoord3 += totalNumOfMessagesCoord3;
            String s4 = "Counter3 => lastBroadcastValue : "+counters3.get(i).getLastBroadcastValue()
                        + " , numOfRounds : "+counters3.get(i).getCounterDoubles()
                        + " , estimator : "+counters3.get(i).getCounter()
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers3
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord3
                        + " , totalMessages : "+totalNumOfMessages3
                        + " , k/epsilon : "+workers3/counters3.get(i).getEpsilon()
                        + " , (k/epsilon)*logN : "+(workers3/counters3.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                        + " ,"+((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+counters3.get(i).getCounter()+" <= "+((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))
                        + " , numDriftInc : "+counters3.get(i).getNumMessages();

            System.out.println(s4);
            writer.write(s4+"\n");

            // Violations on counters
            // if( counters3.get(i).getCounter() > ((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ upperViolation++; }
            // else if( counters3.get(i).getCounter() < ((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ lowerViolation++; }

            // Differences
            String s5 = "Diff => "
                        +" estimator1 : "+counters1.get(i).getCounter()
                        +" , estimator3 : "+counters3.get(i).getCounter()
                        +" , estimatorEXACT : "+calculateEstimator(counters2.get(i));
            System.out.println(s5);
            writer.write(s5+"\n");
            if(counters1.get(i).getCounter() != counters3.get(i).getCounter() ){
                System.out.println("Diff Counter1 and Counters3");
                writer.write("Diff Counter1 and Counters3"+"\n");
            }
            if(counters1.get(i).getCounter() != calculateEstimator(counters2.get(i)) ){
                System.out.println("Diff Counter1 and Counters2");
                writer.write("Diff Counter1 and Counters2"+"\n");
            }
            if(counters3.get(i).getCounter() != calculateEstimator(counters2.get(i)) ){
                System.out.println("Diff Counter3 and Counters2");
                writer.write("Diff Counter3 and Counters2"+"\n");
            }


            // EXACT
            long totalNumOfMessagesWorkers2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessagesWorkers2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages2= totalNumOfMessagesWorkers2+(2*counters2.get(i).getNumMessages()*workers2);
            long totalNumOfMessagesCoord2 = 2*counters2.get(i).getNumMessages()*workers2;
            totalMessagesWorkers2 += totalNumOfMessages2;
            totalMessagesCoord2 += totalNumOfMessagesCoord2;
            String s2 = "CounterEXACT => lastBroadcastValue : "+counters2.get(i).getLastBroadcastValue()
                        + " , estimator : "+calculateEstimator(counters2.get(i))
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers2
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord2
                        + " , totalMessages : "+totalNumOfMessages2;

            System.out.println(s2+"\n");
            writer.write(s2);

            String s3 = "\nLast broadcast value(Counters1) => Coordinator : "+counters1.get(i).getLastBroadcastValue();
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() > counters1.get(i).getLastBroadcastValue()) countBroad++;
                s3 = s3+", "+j+":"+workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue();
            }
            System.out.println(s3+"\n");
            writer.write(s3+"\n");

            // Messages
            if(totalNumOfMessages1 == 0 &&  totalNumOfMessages2 == 0) zeroMes++;
            else if(totalNumOfMessages1 == totalNumOfMessages2) equalMes++;
            else if(totalNumOfMessages1 > totalNumOfMessages2){
                writer.write("Bigger Messages"+"\n");
                biggerMes++;
            }
            else{
                writer.write("Less Messages"+"\n");
                lessMes++;
            }
        }

        System.out.println("Probabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1);
        writer.write("\nProbabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1+"\n");

        System.out.println("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes);
        writer.write("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes+"\n");

        System.out.println("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound);
        writer.write("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound+"\n");

        System.out.println("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad);
        writer.write("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad+"\n");

        System.out.println("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon);
        writer.write("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon+"\n");

        System.out.println("Counters bigger broadcast value : "+countBroad);
        writer.write("Counters bigger broadcast value : "+countBroad+"\n");

        System.out.println("Violations on counters1 => "+"  lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation);
        writer.write("Violations on counters1 => "+" lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation+"\n");

        // Broadcast values and sync
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    System.out.println("Different broadcast value : worker "+j+" => "+counters1.get(i));

                }
            }
        }

        long counterBroad = 0L;
        long countSync = 0L;
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    counterBroad++;
                    break;
                }
            }
        }

        for(int j=0;j<workers1;j++){

            for(int i=0;i<counters1.size();i++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).isSync() ){
                    countSync++;
                }
            }
            System.out.println("Worker "+j+" , counters sync : "+countSync);
            writer.write("Worker "+j+" , counters sync : "+countSync+"\n");
            countSync=0;
        }

        System.out.println("Counters1 different broadcast value : "+counterBroad);
        writer.write("Counters1 different broadcast value : "+counterBroad+"\n");

        System.out.println("Counter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2);
        writer.write("\n\nCounter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2+"\n");

        System.out.println("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1);
        writer.write("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1+"\n");

        System.out.println("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3);
        writer.write("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3+"\n");

        // Close the buffer
        writer.close();
    }

    // Compare the RC and EXACT type
    public static void checkRC(String file1,int workers1,String file2,int workers2,String file3,int workers3,String outputFile) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"stats5"));

        // Counters from the first file
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Counters from the second file
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();


        // Counters from the third file
        Vector<Counter> counters3 = new Vector<>();
        br = new BufferedReader(new FileReader(file3+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters3.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters3 = new LinkedHashMap<>();
        for(int i=0;i<workers3;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file3+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters3.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers3;i++){
            if(workersCounters3.get(i).size() != counters3.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Compare the counters
        System.out.println("Counters1 size : "+counters1.size());
        System.out.println("Counters2 size : "+counters2.size());
        System.out.println("Counters3 size : "+counters3.size());


        long equalProb=0;long lessProb=0;long biggerProb=0;long prob1=0;
        long equalMes=0;long lessMes=0;long biggerMes=0;long zeroMes=0;
        long equalRound=0;long lessRound=0;long biggerRound=0;
        long equalBroad=0;long lessBroad=0;long biggerBroad=0;
        long equalEpsilon=0;long lessEpsilon=0;long biggerEpsilon=0;
        long upperViolation=0;long lowerViolation=0;
        long countBroad=0;
        long totalMessagesWorkers1=0;long totalMessagesCoord1=0;
        long totalMessagesWorkers2=0;long totalMessagesCoord2=0;
        long totalMessagesWorkers3=0;long totalMessagesCoord3=0;
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()
               && counters1.get(i).getCounterKey() != counters3.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Check delta,resConstant,trueParameter
            if( (counters1.get(i).getDelta() != counters2.get(i).getDelta()) &&
                (counters1.get(i).getResConstant() != counters2.get(i).getResConstant()) &&
                (counters1.get(i).getTrueParameter() != counters2.get(i).getTrueParameter()) ){
                System.out.println("Counters not equal");
                continue;
            }

            // Epsilon
            if(counters1.get(i).getEpsilon() == counters3.get(i).getEpsilon()) equalEpsilon++;
            else if(counters1.get(i).getEpsilon() > counters3.get(i).getEpsilon()) biggerEpsilon++;
            else lessEpsilon++;

            // Probabilities
            if(counters1.get(i).getProb() == 1d && counters3.get(i).getProb() == 1d) prob1++;
            else if(counters1.get(i).getProb() == counters3.get(i).getProb()) equalProb++;
            else if(counters1.get(i).getProb() > counters3.get(i).getProb()) biggerProb++;
            else lessProb++;

            // Rounds
            if(counters1.get(i).getNumMessages() == counters3.get(i).getNumMessages()) equalRound++;
            else if(counters1.get(i).getNumMessages() > counters3.get(i).getNumMessages()){ biggerRound++; }
            else lessRound++;

            // Broadcast value
            if(counters1.get(i).getLastBroadcastValue() == counters3.get(i).getLastBroadcastValue()) equalBroad++;
            else if(counters1.get(i).getLastBroadcastValue() > counters3.get(i).getLastBroadcastValue()) biggerBroad++;
            else lessBroad++;

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                        + " , value : " + counters1.get(i).getNodeValue()
                        + " , actualNode : " + counters1.get(i).isActualNode()
                        + " , nodeParents : " + counters1.get(i).getNodeParents()
                        + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                        + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);
            writer.write(s+"\n");

            // Get the total of number of messages for counter
            long totalNumOfMessagesWorkers1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessagesWorkers1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages1= totalNumOfMessagesWorkers1+(counters1.get(i).getNumMessages()*workers1);
            long totalNumOfMessagesCoord1 = counters1.get(i).getNumMessages()*workers1;
            long estimator = calculateEstimator(counters1.get(i));
            totalMessagesWorkers1 += totalNumOfMessagesWorkers1;
            totalMessagesCoord1 += totalNumOfMessagesCoord1;
            String s1 = "Counter1 => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                    + " , sumValues : " + sumOfLastSentValues(counters1.get(i))
                    + " , sumUpdates : " + sumOfUpdates(counters1.get(i))
                    + " , numOfRounds : "+counters1.get(i).getNumMessages()
                    + " , prob : "+counters1.get(i).getProb()
                    + " , estimator : "+estimator
                    + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers1
                    + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord1
                    + " , totalMessages : "+totalNumOfMessages1
                    + " , sqrt(k)/epsilon : "+Math.sqrt(workers1)/counters1.get(i).getEpsilon()
                    + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers1)/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                    + " ,"+((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+estimator+" <= "+((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)));

            System.out.println(s1);
            writer.write(s1+"\n");

            // Violations on counters
            if( estimator > ((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Upper violation \n");
                upperViolation++;
            }
            else if( estimator < ((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Lower violation \n");
                writer.write("Last sent values : "+counters1.get(i).getLastSentValues()+"\n");
                writer.write("Last sent updates : "+counters1.get(i).getLastSentUpdates()+"\n");
                lowerViolation++;
            }


            // Get the total of number of messages for counter from third file
            long totalNumOfMessagesWorkers3=0;
            for(int j=0;j<workers3;j++){
                totalNumOfMessagesWorkers3 += workersCounters3.get(j).get(counters3.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages3= totalNumOfMessagesWorkers3+(counters3.get(i).getNumMessages()*workers3);
            long totalNumOfMessagesCoord3 = counters3.get(i).getNumMessages()*workers3;
            estimator = calculateEstimator(counters3.get(i));
            totalMessagesWorkers3 += totalNumOfMessagesWorkers3;
            totalMessagesCoord3 += totalNumOfMessagesCoord3;
            String s4 = "Counter3 => lastBroadcastValue : "+counters3.get(i).getLastBroadcastValue()
                        + " , sumValues : " + sumOfLastSentValues(counters3.get(i))
                        + " , sumUpdates : " + sumOfUpdates(counters3.get(i))
                        + " , numOfRounds : "+counters3.get(i).getNumMessages()
                        + " , prob : "+counters3.get(i).getProb()
                        + " , estimator : "+estimator
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers3
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord3
                        + " , totalMessages : "+totalNumOfMessages3
                        + " , sqrt(k)/epsilon : "+Math.sqrt(workers3)/counters3.get(i).getEpsilon()
                        + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers3)/counters3.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                        + " ,"+((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+estimator+" <= "+((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)));

            System.out.println(s4);
            writer.write(s4+"\n");

            // Violations on counters
            // if( counters3.get(i).getCounter() > ((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ upperViolation++; }
            // else if( counters3.get(i).getCounter() < ((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ lowerViolation++; }

            // Differences
            long estimator1 = calculateEstimator(counters1.get(i));
            long estimator2 = calculateEstimator(counters2.get(i));
            long estimator3 = calculateEstimator(counters3.get(i));
            String s5 = "Diff => "
                        +" estimator1 : "+estimator1
                        +" , estimator3 : "+estimator3
                        +" , estimatorEXACT : "+estimator2;
            System.out.println(s5);
            writer.write(s5+"\n");
            if(estimator1 != estimator3 ){
                System.out.println("Diff Counter1 and Counters3");
                writer.write("Diff Counter1 and Counters3"+"\n");
            }
            if(estimator1 != estimator2 ){
                System.out.println("Diff Counter1 and Counters2");
                writer.write("Diff Counter1 and Counters2"+"\n");
            }
            if(estimator3 != estimator2 ){
                System.out.println("Diff Counter3 and Counters2");
                writer.write("Diff Counter3 and Counters2"+"\n");
            }


            // EXACT
            long totalNumOfMessagesWorkers2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessagesWorkers2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages2= totalNumOfMessagesWorkers2+(counters2.get(i).getNumMessages()*workers2);
            long totalNumOfMessagesCoord2 = counters2.get(i).getNumMessages()*workers2;
            totalMessagesWorkers2 += totalNumOfMessages2;
            totalMessagesCoord2 += totalNumOfMessagesCoord2;
            String s2 = "CounterEXACT => lastBroadcastValue : "+counters2.get(i).getLastBroadcastValue()
                        + " , estimator : "+calculateEstimator(counters2.get(i))
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers2
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord2
                        + " , totalMessages : "+totalNumOfMessages2;

            System.out.println(s2+"\n");
            writer.write(s2);

            String s3 = "\nLast broadcast value(Counters1) => Coordinator : "+counters1.get(i).getLastBroadcastValue();
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() > counters1.get(i).getLastBroadcastValue()) countBroad++;
                s3 = s3+", "+j+":"+workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue();
            }
            System.out.println(s3+"\n");
            writer.write(s3+"\n");

            // Messages
            if(totalNumOfMessages1 == 0 &&  totalNumOfMessages2 == 0) zeroMes++;
            else if(totalNumOfMessages1 == totalNumOfMessages2) equalMes++;
            else if(totalNumOfMessages1 > totalNumOfMessages2){
                writer.write("Bigger Messages"+"\n");
                biggerMes++;
            }
            else{
                writer.write("Less Messages"+"\n");
                lessMes++;
            }
        }

        System.out.println("Probabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1);
        writer.write("\nProbabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1+"\n");

        System.out.println("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes);
        writer.write("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes+"\n");

        System.out.println("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound);
        writer.write("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound+"\n");

        System.out.println("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad);
        writer.write("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad+"\n");

        System.out.println("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon);
        writer.write("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon+"\n");

        System.out.println("Counters bigger broadcast value : "+countBroad);
        writer.write("Counters bigger broadcast value : "+countBroad+"\n");

        System.out.println("Violations on counters1 => "+"  lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation);
        writer.write("Violations on counters1 => "+" lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation+"\n");

        // Broadcast values and sync
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    System.out.println("Different broadcast value : worker "+j+" => "+counters1.get(i));

                }
            }
        }

        long counterBroad = 0L;
        long countSync = 0L;
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    counterBroad++;
                    break;
                }
            }
        }

        for(int j=0;j<workers1;j++){

            for(int i=0;i<counters1.size();i++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).isSync() ){
                    countSync++;
                }
            }
            System.out.println("Worker "+j+" , counters sync : "+countSync);
            writer.write("Worker "+j+" , counters sync : "+countSync+"\n");
            countSync=0;
        }

        System.out.println("Counters1 different broadcast value : "+counterBroad);
        writer.write("Counters1 different broadcast value : "+counterBroad+"\n");

        // Counters1
        System.out.println("\nCounters1");
        for (Counter counter : counters1) {
            for(int i=0;i<workers1;i++){
                if (counter.getProb() != workersCounters1.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        // Counters2
        System.out.println("\nCounters2");
        for (Counter counter : counters2) {
            for(int i=0;i<workers2;i++){
                if (counter.getProb() != workersCounters2.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        System.out.println("Counter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2);
        writer.write("\n\nCounter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2+"\n");

        System.out.println("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1);
        writer.write("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1+"\n");

        System.out.println("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3);
        writer.write("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3+"\n");

        // Close the buffer
        writer.close();
    }

    // Compare the RC,DC and EXACT type
    public static void checkRCDC(String file1,int workers1,String file2,int workers2,String file3,int workers3,String outputFile) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"stats4"));

        // Counters from the first file-RC
        Vector<Counter> counters1 = new Vector<>();
        BufferedReader br = new BufferedReader(new FileReader(file1+"\\coordinator_stats.txt"));
        String line = br.readLine();
        boolean find = false;

        // Skip the lines until reach end of processing and get the counters from the first file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters1.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters1 = new LinkedHashMap<>();
        for(int i=0;i<workers1;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file1+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters1.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers1;i++){
            if(workersCounters1.get(i).size() != counters1.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Counters from the second file-EXACT
        Vector<Counter> counters2 = new Vector<>();
        br = new BufferedReader(new FileReader(file2+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters2.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters2 = new LinkedHashMap<>();
        for(int i=0;i<workers2;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file2+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters2.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers2;i++){
            if(workersCounters2.get(i).size() != counters2.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();


        // Counters from the third file-DC
        Vector<Counter> counters3 = new Vector<>();
        br = new BufferedReader(new FileReader(file3+"\\coordinator_stats.txt"));
        line = br.readLine();
        find = false;

        // Skip the lines until reach end of processing and get the counters from the second file(Coordinator)
        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            if(line.contains("End of processing")) find = true;

            if(find) {
                if (line.contains("Counter =>")) {

                    // Get the counter
                    Counter counter = strToCounter(line);

                    // Get the key
                    String key = getKey(counter);

                    // Hash the key
                    long hashedKey = hashKey(key.getBytes());

                    // Update the counter
                    counter.setCounterKey(hashedKey);

                    // Add the counter
                    counters3.add(counter);
                }
            }

            // Read the next line
            line = br.readLine();

        }

        // Close the buffer
        br.close();

        // Get the counters of workers
        LinkedHashMap<Integer,LinkedHashMap<Long,Counter>> workersCounters3 = new LinkedHashMap<>();
        for(int i=0;i<workers3;i++){

            LinkedHashMap<Long,Counter> counters = new LinkedHashMap<>();
            br = new BufferedReader(new FileReader(file3+"\\worker_stats_"+i+".txt"));
            line = br.readLine();
            find = false;
            while ( line != null ) {

                line = line.replaceAll("\0", "");

                // Skip new and empty lines
                if (line.equals("\n") || line.equals("")) {
                    line = br.readLine();
                    continue;
                }

                if(line.contains("End of process")) find = true;

                if(find) {
                    if (!line.contains("Worker state") && line.contains("Counter =>")) {

                        // Get the counter
                        Counter counter = strToCounter(line);

                        // Get the key
                        String key = getKey(counter);

                        // Hash the key
                        long hashedKey = hashKey(key.getBytes());

                        // Update the counter
                        counter.setCounterKey(hashedKey);

                        // Add the counter
                        counters.put(hashedKey,counter);
                    }
                }

                // Read the next line
                line = br.readLine();

            }

            // Close the buffer
            br.close();

            workersCounters3.put(i,counters);
        }

        // Check the size of counters
        for(int i=0;i<workers3;i++){
            if(workersCounters3.get(i).size() != counters3.size()){
                System.out.println("Not equal size per worker");
                return;
            }
        }

        // Close the buffer
        br.close();



        // Compare the counters
        System.out.println("Counters1 size : "+counters1.size());
        System.out.println("Counters2 size : "+counters2.size());
        System.out.println("Counters3 size : "+counters3.size());


        long equalProb=0;long lessProb=0;long biggerProb=0;long prob1=0;
        long equalMes=0;long lessMes=0;long biggerMes=0;long zeroMes=0;
        long equalRound=0;long lessRound=0;long biggerRound=0;
        long equalBroad=0;long lessBroad=0;long biggerBroad=0;
        long equalEpsilon=0;long lessEpsilon=0;long biggerEpsilon=0;
        long upperViolation=0;long lowerViolation=0;
        long countBroad=0;
        long totalMessagesWorkers1=0;long totalMessagesCoord1=0;
        long totalMessagesWorkers2=0;long totalMessagesCoord2=0;
        long totalMessagesWorkers3=0;long totalMessagesCoord3=0;
        for(int i=0;i<counters1.size();i++){

            if(counters1.get(i).getCounterKey() != counters2.get(i).getCounterKey()
               && counters1.get(i).getCounterKey() != counters3.get(i).getCounterKey()){
                System.out.println("Counters not equal");
                System.out.println("Counter1 : "+counters1.get(i));
                System.out.println("Counter2 : "+counters2.get(i));
                continue;
            }

            // Epsilon
            if(counters1.get(i).getEpsilon() == counters3.get(i).getEpsilon()) equalEpsilon++;
            else if(counters1.get(i).getEpsilon() > counters3.get(i).getEpsilon()) biggerEpsilon++;
            else lessEpsilon++;

            // Probabilities
            if(counters1.get(i).getProb() == 1d && counters2.get(i).getProb() == 1d) prob1++;
            else if(counters1.get(i).getProb() == counters2.get(i).getProb()) equalProb++;
            else if(counters1.get(i).getProb() > counters2.get(i).getProb()) biggerProb++;
            else lessProb++;

            // Rounds
            if(counters1.get(i).getNumMessages() == counters3.get(i).getCounterDoubles()) equalRound++;
            else if(counters1.get(i).getNumMessages() > counters3.get(i).getCounterDoubles()) biggerRound++;
            else lessRound++;

            // Broadcast value
            if(counters1.get(i).getLastBroadcastValue() == counters3.get(i).getLastBroadcastValue()) equalBroad++;
            else if(counters1.get(i).getLastBroadcastValue() > counters3.get(i).getLastBroadcastValue()) biggerBroad++;
            else lessBroad++;

            // Prints
            String s = "\n\nCounter => name : " + counters1.get(i).getNodeName()
                        + " , value : " + counters1.get(i).getNodeValue()
                        + " , actualNode : " + counters1.get(i).isActualNode()
                        + " , nodeParents : " + counters1.get(i).getNodeParents()
                        + " , nodeParentValues : " + counters1.get(i).getNodeParentValues()
                        + " , counter_value : " + ( counters1.get(i).getNodeParentValues() == null ? counters1.get(i).getNodeValue() : counters1.get(i).getNodeValue().toString().concat(counters1.get(i).getNodeParentValues().toString()));

            System.out.println(s);
            writer.write(s+"\n");

            // Get the total of number of messages for counter
            long totalNumOfMessagesWorkers1=0;
            for(int j=0;j<workers1;j++){
                totalNumOfMessagesWorkers1 += workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages1= totalNumOfMessagesWorkers1+(counters1.get(i).getNumMessages()*workers1);
            long totalNumOfMessagesCoord1 = counters1.get(i).getNumMessages()*workers1;
            long estimator = calculateEstimator(counters1.get(i));
            totalMessagesWorkers1 += totalNumOfMessagesWorkers1;
            totalMessagesCoord1 += totalNumOfMessagesCoord1;
            String s1 = "Counter1(RC) => lastBroadcastValue : "+counters1.get(i).getLastBroadcastValue()
                        + " , sumValues : " + sumOfLastSentValues(counters1.get(i))
                        + " , sumUpdates : " + sumOfUpdates(counters1.get(i))
                        + " , numOfRounds : "+counters1.get(i).getNumMessages()
                        + " , prob : "+counters1.get(i).getProb()
                        + " , estimator : "+estimator
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers1
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord1
                        + " , totalMessages : "+totalNumOfMessages1
                        + " , sqrt(k)/epsilon : "+Math.sqrt(workers1)/counters1.get(i).getEpsilon()
                        + " , (sqrt(k)/epsilon)*logN : "+(Math.sqrt(workers1)/counters1.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                        + " , condition : " + counters1.get(i).getLastBroadcastValue()/calculate(workers1,counters1.get(i).getEpsilon(),counters1.get(i).getResConstant())
                        + " ,"+((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+estimator+" <= "+((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i)));

            System.out.println(s1);
            writer.write(s1+"\n");

            // Violations on counters
            if( estimator > ((1+counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Upper violation \n");
                upperViolation++;
            }
            else if( estimator < ((1-counters1.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){
                writer.write("Lower violation \n");
                lowerViolation++;
            }


            // Get the total of number of messages for counter from third file
            long totalNumOfMessagesWorkers3=0;
            for(int j=0;j<workers3;j++){
                totalNumOfMessagesWorkers3 += workersCounters3.get(j).get(counters3.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages3= totalNumOfMessagesWorkers3+(2*counters3.get(i).getCounterDoubles()*workers3);
            long totalNumOfMessagesCoord3 = 2*counters3.get(i).getCounterDoubles()*workers3;
            estimator = counters3.get(i).getCounter();
            totalMessagesWorkers3 += totalNumOfMessagesWorkers3;
            totalMessagesCoord3 += totalNumOfMessagesCoord3;
            String s4 = "Counter3(DC) => lastBroadcastValue : "+counters3.get(i).getLastBroadcastValue()
                    + " , numOfRounds : "+counters3.get(i).getCounterDoubles()
                    + " , estimator : "+estimator
                    + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers3
                    + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord3
                    + " , totalMessages : "+totalNumOfMessages3
                    + " , k/epsilon : "+workers3/counters3.get(i).getEpsilon()
                    + " , (k/epsilon)*logN : "+(workers3/counters3.get(i).getEpsilon())*log2(calculateEstimator(counters2.get(i)))
                    + " , condition : "+MathUtils.calculateCondition(counters3.get(i).getEpsilon(),counters3.get(i).getLastBroadcastValue(),workers3)
                    + " ,"+((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)))+" <= estimator = "+estimator+" <= "+((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i)));

            System.out.println(s4);
            writer.write(s4+"\n");

            // Violations on counters
            // if( counters3.get(i).getCounter() > ((1+counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ upperViolation++; }
            // else if( counters3.get(i).getCounter() < ((1-counters3.get(i).getEpsilon())*calculateEstimator(counters2.get(i))) ){ lowerViolation++; }

            // Differences
            long estimator1 = calculateEstimator(counters1.get(i));
            long estimator2 = calculateEstimator(counters2.get(i));
            long estimator3 = counters3.get(i).getCounter();
            String s5 = "Diff => "
                        +" estimator1(RC) : "+estimator1
                        +" , estimator3 (DC): "+estimator3
                        +" , estimatorEXACT : "+estimator2;
            System.out.println(s5);
            writer.write(s5+"\n");
            if(estimator1 != estimator3 ){
                System.out.println("Diff Counter1 and Counters3");
                writer.write("Diff Counter1 and Counters3"+"\n");
            }
            if(estimator1 != estimator2 ){
                System.out.println("Diff Counter1 and Counters2");
                writer.write("Diff Counter1 and Counters2"+"\n");
            }
            if(estimator3 != estimator2 ){
                System.out.println("Diff Counter3 and Counters2");
                writer.write("Diff Counter3 and Counters2"+"\n");
            }


            // EXACT
            long totalNumOfMessagesWorkers2 = 0;
            for(int j=0;j<workers2;j++){
                totalNumOfMessagesWorkers2 += workersCounters2.get(j).get(counters2.get(i).getCounterKey()).getNumMessages();
            }
            long totalNumOfMessages2= totalNumOfMessagesWorkers2+(counters2.get(i).getNumMessages()*workers2);
            long totalNumOfMessagesCoord2 = counters2.get(i).getNumMessages()*workers2;
            totalMessagesWorkers2 += totalNumOfMessages2;
            totalMessagesCoord2 += totalNumOfMessagesCoord2;
            String s2 = "CounterEXACT => lastBroadcastValue : "+counters2.get(i).getLastBroadcastValue()
                        + " , estimator : "+calculateEstimator(counters2.get(i))
                        + " , numOfMessagesWorkers : "+totalNumOfMessagesWorkers2
                        + " , numOfMessagesCoordinator : "+totalNumOfMessagesCoord2
                        + " , totalMessages : "+totalNumOfMessages2;

            System.out.println(s2+"\n");
            writer.write(s2);

            String s3 = "\nLast broadcast value(Counters1) => Coordinator : "+counters1.get(i).getLastBroadcastValue();
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() > counters1.get(i).getLastBroadcastValue()) countBroad++;
                s3 = s3+", "+j+":"+workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue();
            }
            System.out.println(s3+"\n");
            writer.write(s3+"\n");

            // Messages
            if(totalNumOfMessages1 == 0 &&  totalNumOfMessages3 == 0) zeroMes++;
            else if(totalNumOfMessages1 == totalNumOfMessages3) equalMes++;
            else if(totalNumOfMessages1 > totalNumOfMessages3){
                writer.write("Bigger Messages"+"\n");
                biggerMes++;
            }
            else{
                writer.write("Less Messages"+"\n");
                lessMes++;
            }
        }

        System.out.println("Probabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1);
        writer.write("\nProbabilities => equalProb : "+equalProb+" , lessProb : "+lessProb+" , biggerProb : "+biggerProb+" , prob=1 : "+prob1+"\n");

        System.out.println("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes);
        writer.write("Messages => equalMes : "+equalMes+" , lessMes : "+lessMes+" , biggerMes : "+biggerMes+" , zeroMes : "+zeroMes+"\n");

        System.out.println("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound);
        writer.write("Rounds => equalRound : "+equalRound+" , lessRound : "+lessRound+" , biggerRound : "+biggerRound+"\n");

        System.out.println("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad);
        writer.write("Broadcast => equalBroad : "+equalBroad+" , lessBroad : "+lessBroad+" , biggerBroad : "+biggerBroad+"\n");

        System.out.println("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon);
        writer.write("Epsilon => equalEpsilon : "+equalEpsilon+" , lessEpsilon "+lessEpsilon+" , biggerEpsilon : "+biggerEpsilon+"\n");

        System.out.println("Counters bigger broadcast value : "+countBroad);
        writer.write("Counters bigger broadcast value : "+countBroad+"\n");

        System.out.println("Violations on counters1 => "+"  lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation);
        writer.write("Violations on counters1 => "+" lowerViolation "+lowerViolation+" , upperViolation : "+upperViolation+"\n");

        // Broadcast values and sync
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    System.out.println("Different broadcast value : worker "+j+" => "+counters1.get(i));

                }
            }
        }

        long counterBroad = 0L;
        long countSync = 0L;
        for(int i=0;i<counters1.size();i++){

            long lastBroadcastValue = counters1.get(i).getLastBroadcastValue();

            // Check lastBroadcast value
            for(int j=0;j<workers1;j++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).getLastBroadcastValue() != lastBroadcastValue){
                    counterBroad++;
                    break;
                }
            }
        }

        for(int j=0;j<workers1;j++){

            for(int i=0;i<counters1.size();i++){
                if( workersCounters1.get(j).get(counters1.get(i).getCounterKey()).isSync() ){
                    countSync++;
                }
            }
            System.out.println("Worker "+j+" , counters sync : "+countSync);
            writer.write("Worker "+j+" , counters sync : "+countSync+"\n");
            countSync=0;
        }

        System.out.println("Counters1 different broadcast value : "+counterBroad);
        writer.write("Counters1 different broadcast value : "+counterBroad+"\n");

        // Counters1
        System.out.println("\nCounters1");
        for (Counter counter : counters1) {
            for(int i=0;i<workers1;i++){
                if (counter.getProb() != workersCounters1.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        // Counters2
        System.out.println("\nCounters2");
        for (Counter counter : counters2) {
            for(int i=0;i<workers2;i++){
                if (counter.getProb() != workersCounters2.get(i).get(counter.getCounterKey()).getProb()) {
                    System.out.println(counter);
                }
            }
        }

        System.out.println("Counter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2);
        writer.write("\n\nCounter2 => Total number of messages from workers : "+totalMessagesWorkers2+" , coordinator : "+totalMessagesCoord2+"\n");

        System.out.println("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1);
        writer.write("Counter1 => Total number of messages from workers : "+totalMessagesWorkers1+" , coordinator : "+totalMessagesCoord1+"\n");

        System.out.println("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3);
        writer.write("Counter3 => Total number of messages from workers : "+totalMessagesWorkers3+" , coordinator : "+totalMessagesCoord3+"\n");

        // Close the buffer
        writer.close();
    }




    // Collect missing files from runs
    public static void collectMissingFiles(String inputFile,String typeExp,int numOfRuns,int numWorkers,int parallelism,boolean checkWorkerStats) throws IOException {


        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM"};
        // String[] types = {"RANDOMIZED","DETERMINISTIC"};

        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};
        // String[] setup = {"BASELINE"};

        // Runs
        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        ArrayList<String> experiment = new ArrayList<>();
        if(typeExp.contains("epsilon")){

            String epsilon;
            for(double eps=0.02;eps<0.14;eps+=0.02) {

                if (eps == 0.1d) epsilon = eps + "0";
                else if (eps >= 0.12d) {
                    epsilon = String.valueOf(eps);
                    epsilon = epsilon.substring(0, 4);
                }
                else epsilon = String.valueOf(eps);
                experiment.add("eps="+epsilon);
            }
        }
        else if(typeExp.contains("workers")){

            // For each worker
            int flag;
            for(int worker=2;worker<=numWorkers;worker+=2) {

                flag = worker;
                if (worker == 14) { worker = 16; }
                else if (worker == 16) { worker = 20; }
                else if(worker == 18){ worker = 26; }

                // Workers >= 26 , numWorkers = 66
                // if( worker != 32 && worker != 38 && worker != 44 && worker != 52 && worker != 60 && worker != 64 ){ continue; }

                experiment.add("workers="+worker);

                // Reset worker
                worker=flag;
            }
        }
        else if(typeExp.contains("parallelism")){

            // For each worker
            int flag;
            for(int par=1;par<=parallelism;par+=1) {

                flag = par;
                // if (par == 14) { par = 20; }
                // if (par == 16) { par = 26; }

                // Step=1,17
                // if( (par % 2) != 0 && (par % 3) != 0 ){ continue; }
                // if( (par > 12 && par<=15) || par == 17 ){ continue; }

                // Parallelism = 44,step=1
                // if (par != 3 && par != 6 && par != 8 && par != 10 && par != 12 && par != 42) { continue;}

                // Parallelism = 15,step=1
                // if(par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 8 && par != 10 && par != 12 && par != 15){ continue; }

                // Parallelism = 16,step=1
                if( par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 7 && par != 9 && par != 10 && par != 12 && par != 14 &&  par != 16){ continue; }

                experiment.add("parallelism="+par);

                // Reset par
                par=flag;
            }
        }
        else if(typeExp.contains("dataset")){
            experiment.add("5K");
            experiment.add("50K");
            experiment.add("500K");
            experiment.add("5M");
            experiment.add("50M");
        }

        // For each type
        for (String run : runs) {
            for (String type : types) {
                // For each error setup
                for (String error : setup) {
                    // For each value of experiment
                    for (String exp : experiment) {

                        if (type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                        // Create the input file
                        String newInputFile = inputFile + run + "\\" + type + "\\" + error + "\\" + exp;
                        // String newInputFile = inputFile + "5M\\" + run + "\\" + type + "\\" + error + "\\" + exp;

                        String coordinatorStats = newInputFile + "\\coordinator_stats.txt";
                        if (typeExp.contains("workers")) {

                            // Check coordinator stats
                            try {
                                FileReader fileReader = new FileReader(coordinatorStats);
                                fileReader.close();
                            }
                            catch (java.io.FileNotFoundException e) {
                                System.out.println("File not found : " + coordinatorStats);
                            }

                            // Check workers stats
                            if(checkWorkerStats) {
                                int workers = Integer.parseInt(exp.split("=")[1]);
                                for (int i = 0; i < workers; i++) {
                                    String workerStats = newInputFile + "\\worker_stats_" + i + ".txt";
                                    try {
                                        FileReader fileReader = new FileReader(workerStats);
                                        fileReader.close();
                                    } catch (java.io.FileNotFoundException e) {
                                        System.out.println("File not found : " + workerStats);
                                    }
                                }
                            }
                        }
                        else {
                            // Check coordinator stats
                            try {
                                FileReader fileReader = new FileReader(coordinatorStats);
                                fileReader.close();
                            }
                            catch (java.io.FileNotFoundException e) {
                                System.out.println("File not found : " + coordinatorStats);
                            }

                            // Check workers stats
                            if(checkWorkerStats) {
                                for (int i = 0; i < numWorkers; i++) {
                                    String workerStats = newInputFile + "\\worker_stats_" + i + ".txt";
                                    try {
                                        FileReader fileReader = new FileReader(workerStats);
                                        fileReader.close();
                                    } catch (java.io.FileNotFoundException e) {
                                        System.out.println("File not found : " + workerStats);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // EXACT
        for (String run : runs) {
            for (String exp : experiment) {
                // Create the input file
                String newInputFile = inputFile + run + "\\EXACT\\" + exp;

                String coordinatorStats = newInputFile + "\\coordinator_stats.txt";
                if (typeExp.contains("workers")) {

                    // Check coordinator stats
                    try {
                        FileReader fileReader = new FileReader(coordinatorStats);
                        fileReader.close();
                    }
                    catch (java.io.FileNotFoundException e) {
                        System.out.println("File not found : " + coordinatorStats);
                    }

                    // Check workers stats
                    if(checkWorkerStats) {
                        int workers = Integer.parseInt(exp.split("=")[1]);
                        for (int i = 0; i < workers; i++) {
                            String workerStats = newInputFile + "\\worker_stats_" + i + ".txt";
                            try {
                                FileReader fileReader = new FileReader(workerStats);
                                fileReader.close();
                            } catch (java.io.FileNotFoundException e) {
                                System.out.println("File not found : " + workerStats);
                            }
                        }
                    }
                }
                else {
                    // Check coordinator stats
                    try {
                        FileReader fileReader = new FileReader(coordinatorStats);
                        fileReader.close();
                    }
                    catch (java.io.FileNotFoundException e) {
                        System.out.println("File not found : " + coordinatorStats);
                    }

                    // Check workers stats
                    if(checkWorkerStats) {
                        for (int i = 0; i < numWorkers; i++) {
                            String workerStats = newInputFile + "\\worker_stats_" + i + ".txt";
                            try {
                                FileReader fileReader = new FileReader(workerStats);
                                fileReader.close();
                            } catch (java.io.FileNotFoundException e) {
                                System.out.println("File not found : " + workerStats);
                            }
                        }
                    }
                }
            }
        }

    }


    // Collect stats from runs
    public static void collectAggregatedStatsRuns(String inputFile,String outputFile,String typeExp) throws IOException {


        // String[] types = {"RANDOMIZED","EXACT"};
        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM","EXACT"};

        // String[] setup = {"BASELINE"};
        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};

        ArrayList<String> experiment = new ArrayList<>();
        if(typeExp.contains("epsilon")){

            String epsilon;
            for(double eps=0.02;eps<0.14;eps+=0.02) {

                if (eps == 0.1d) epsilon = eps + "0";
                else if (eps >= 0.12d) {
                    epsilon = String.valueOf(eps);
                    epsilon = epsilon.substring(0, 4);
                }
                else epsilon = String.valueOf(eps);
                experiment.add("eps="+epsilon);
            }
        }
        else if(typeExp.contains("workers")){

            // For each worker
            int flag;
            for(int worker=2;worker<=66;worker+=2) {

                flag = worker;
                if (worker == 14) { worker = 16; }
                else if (worker == 16) { worker = 20; }
                else if(worker == 18){ worker = 26; }

                // Workers >= 26 , numWorkers = 66
                // if( worker != 32 && worker != 38 && worker != 44 && worker != 52 && worker != 60 && worker != 64 ){ continue; }

                experiment.add("workers="+worker);

                // Reset worker
                worker=flag;
            }
        }
        else if(typeExp.contains("dataset")){
            experiment.add("5K");
            experiment.add("50K");
            experiment.add("500K");
            experiment.add("5M");
            experiment.add("50M");
            // experiment.add("100M");
            // experiment.add("dataset");
        }
        else if(typeExp.contains("parallelism")){

            // For each worker
            int flag;
            for(int par=1;par<=16;par+=1) {

                flag = par;
                // if (par == 14) { par = 20; }
                // if (par == 16) { par = 26; }

                // Step=1,17
                // if( (par % 2) != 0 && (par % 3) != 0 ){ continue; }
                // if( (par > 12 && par<=15) ){ continue; }

                // Parallelism = 44,step=1
                // if(par != 3 && par != 6 && par != 8 && par != 10 && par != 12 && par != 42 ) { continue; }

                // Parallelism = 15,step=1
                // if(par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 8 && par != 10 && par != 12 && par != 15){ continue; }

                // Parallelism = 16,step=1
                if( par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 7 && par != 9 && par != 10 && par != 12 && par != 14 &&  par != 16){ continue; }

                experiment.add("parallelism="+par);

                // Reset par
                par=flag;
            }
        }

        // For each type
        for(String type : types){

            if(type.equals("EXACT")){
                // Modify the array of setup
                setup = new String[1];
                setup[0] = "";

                if(typeExp.contains("epsilon")) continue;

            }

            // For each error setup
            for (String error: setup) {

                ArrayList<String> totalCommunicationCost = new ArrayList<>();
                ArrayList<String> upCommunicationCost = new ArrayList<>();
                ArrayList<String> downCommunicationCost = new ArrayList<>();

                ArrayList<Double> throughputFirst = new ArrayList<>();
                ArrayList<Double> throughputSecond = new ArrayList<>();
                ArrayList<Double> throughputThird = new ArrayList<>();
                ArrayList<Double> throughputInstant = new ArrayList<>();

                ArrayList<Double> errorGT = new ArrayList<>();

                ArrayList<Double> errorMLE = new ArrayList<>();
                ArrayList<Double> lowerViolations = new ArrayList<>();
                ArrayList<Double> upViolations = new ArrayList<>();

                ArrayList<Double> runtime = new ArrayList<>();

                // For each value of experiment
                for ( String exp : experiment) {

                    if (type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                    // Create the input file
                    String newInputFile = inputFile+type+"_"+error+"_"+exp;
                    // String newInputFile = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\five_runs_epsilon_500K\\run_0\\DETERMINISTIC\\BASELINE\\eps=0.02\\communication_cost";
                    System.out.println("Path : "+newInputFile);

                    // Skip file if does not exist
                    try {
                        FileReader fileReader = new FileReader(newInputFile);
                        fileReader.close();
                    }
                    catch (java.io.FileNotFoundException e) {
                        continue;
                    }

                    // Read file line by line and get the results
                    BufferedReader br = new BufferedReader(new FileReader(newInputFile));
                    String line = br.readLine();

                    while (line != null) {

                        line = line.replaceAll("\0", "");

                        // Skip new and empty lines
                        if (line.equals("\n") || line.equals("")) {
                            line = br.readLine();
                            continue;
                        }


                        // Get average upstream communication cost
                        if (line.contains("Average size of upstream(bytes) of")) {
                            upCommunicationCost.add(line.split(" : ")[1].trim());
                        }

                        // Get average downstream communication cost
                        if (line.contains("Average size of downstream(bytes) of")) {
                            downCommunicationCost.add(line.split(" : ")[1].trim());
                        }

                        // Get average communication cost
                        if (line.contains("Average total communication cost(bytes) of ")) {
                            totalCommunicationCost.add(line.split(" : ")[1].trim());
                        }


                        // Get average error to GT
                        if (line.contains("Average absolute error(GT) of")) {
                            errorGT.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }


                        // Get average error to EXACT MLE
                        if (line.contains("Average absolute error(EXACTMLE) of")) {
                            errorMLE.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }
                        if (line.contains("Average of lower violations of runs :")) {
                            lowerViolations.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }
                        if (line.contains("Average of upper violations of runs :")) {
                            upViolations.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }


                        // Get the average runtime
                        if (line.contains("Average runtime(sec) of")) {
                            runtime.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }


                        // Get the average throughput(first-way)
                        if (line.contains("Average first-way throughput(events/sec) of ") ) {
                            throughputFirst.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get the average throughput(second-way)
                        if (line.contains("Average second-way throughput(events/sec) of") ) {
                            throughputSecond.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get the average throughput(third-way)
                        if (line.contains("Average third-way throughput(events/sec) of") ) {
                            throughputThird.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get the average instant throughput
                        if (line.contains("Average instant throughput(events/sec) of") ) {
                            throughputInstant.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }


                        // Read the next line
                        line = br.readLine();

                    }

                    // Close buffer
                    br.close();

                }

                // Write the result to output
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + "aggregated_" + type + "_" + error + "_" + typeExp));

                // Communication cost
                writer.write("Size of upstream(bytes) : " + upCommunicationCost + "\n\n");

                writer.write("Size of downstream(bytes) : " + downCommunicationCost + "\n\n");

                writer.write("Total Communication cost(bytes) : " + totalCommunicationCost + "\n\n");


                // Error to GT
                writer.write("Average absolute error(GT) : " + errorGT + "\n\n");
                // writer.write("Average absolute error(GT) of five runs : " + errorGT.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");


                // Error to EXACTMLE
                writer.write("Average absolute error(EXACTMLE) : " + errorMLE + "\n\n");
                // writer.write("Average absolute error(EXACTMLE) of five runs : " + errorMLE.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");
                writer.write("Lower violations : " + lowerViolations + "\n");
                writer.write("Upper violations : " + upViolations + "\n\n");


                // Runtime
                writer.write("Runtime(sec) : " + runtime + "\n\n");
                // writer.write("Average runtime(sec) of five runs : " + runtime.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");


                // Throughput
                writer.write("First-way Throughput(events/sec) : " + throughputFirst + "\n\n");
                // writer.write("Average first-way throughput(events/sec) of five runs : " + throughputFirst.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Second-way Throughput(events/sec) : " + throughputSecond + "\n\n");
                // writer.write("Average second-way throughput(events/sec) of five runs : " + throughputSecond.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Third-way Throughput(events/sec) : " + throughputThird + "\n\n");
                // writer.write("Average third-way throughput(events/sec) of five runs : " + throughputThird.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Fourth-way Throughput(events/sec) : " + throughputInstant + "\n\n");
                // writer.write("Average fourth-way throughput(events/sec) of five runs : " + throughputInstant.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");


                // Close the buffer
                writer.close();
            }
        }

    }

    // Collect stats from runs
    public static void collectStatsRuns(String inputFile,String outputFile,String typeExp,int numOfRuns,boolean enableWorkersThroughputs) throws IOException {


        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM","EXACT"};
        // String[] types = {"RANDOMIZED"};

        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};
        // String[] setup = {"BASELINE"};

        // Runs
        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        ArrayList<String> experiment = new ArrayList<>();
        if(typeExp.contains("epsilon")){

            String epsilon;
            for(double eps=0.02;eps<0.14;eps+=0.02) {

                if (eps == 0.1d) epsilon = eps + "0";
                else if (eps >= 0.12d) {
                    epsilon = String.valueOf(eps);
                    epsilon = epsilon.substring(0, 4);
                }
                else epsilon = String.valueOf(eps);
                experiment.add("eps="+epsilon);
            }
        }
        else if(typeExp.contains("workers")){

            // For each worker
            int flag;
            for(int worker=2;worker<=66;worker+=2) {

                flag = worker;
                if (worker == 14) { worker = 16; }
                else if (worker == 16) { worker = 20; }
                else if(worker == 18){ worker = 26; }

                // Workers >= 26 , numWorkers = 66
                // if( worker != 32 && worker != 38 && worker != 44 && worker != 52 && worker != 60 && worker != 64 ){ continue; }

                experiment.add("workers="+worker);

                // Reset worker
                worker=flag;
            }
        }
        else if(typeExp.contains("dataset")){
            experiment.add("5K");
            experiment.add("50K");
            experiment.add("500K");
            experiment.add("5M");
            experiment.add("50M");
            // experiment.add("100M");
        }
        else if(typeExp.contains("parallelism")){

            // For each worker
            int flag;
            for(int par=1;par<=16;par+=1) {

                flag = par;
                // if (par == 14) { par = 20; }
                // if (par == 16) { par = 26; }

                // Step=1,17
                // if( (par % 2) != 0 && (par % 3) != 0 ){ continue; }
                // if( (par > 12 && par<=15) ){ continue; }

                // Parallelism = 44,step=1
                // if(par != 3 && par != 6 && par != 8 && par != 10 && par != 12 && par != 42 ) { continue; }

                // Parallelism = 15,step=1
                // if(par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 8 && par != 10 && par != 12 && par != 15){ continue; }

                // Parallelism = 16,step=1
                if( par != 1 && par != 2 && par != 3 && par != 4 && par != 6 && par != 7 && par != 9 && par != 10 && par != 12 && par != 14 &&  par != 16){ continue; }

                experiment.add("parallelism="+par);

                // Reset par
                par=flag;
            }
        }

        // For each type
        for(String type : types){

            // EXACT
            if(type.equals("EXACT")){
                // Modify the array of setup and runs
                setup = new String[1];
                setup[0] = "";

                runs = new ArrayList<>();
                runs.add("run_0");

                /*if(typeExp.contains("parallelism")){
                    runs = new ArrayList<>();
                    runs.add("run_4");
                    inputFile = inputFile+"run_4\\";
                }*/

                if(typeExp.contains("epsilon")) continue;

            }

            // For each error setup
            for (String error: setup) {
                // For each value of experiment
                for ( String exp : experiment) {

                    ArrayList<Long> totalCommunicationCost = new ArrayList<>();
                    ArrayList<Long> upCommunicationCost = new ArrayList<>();
                    ArrayList<Long> downCommunicationCost = new ArrayList<>();

                    ArrayList<Double> throughputFirst = new ArrayList<>();
                    ArrayList<Double> throughputSecond = new ArrayList<>();
                    ArrayList<Double> throughputThird = new ArrayList<>();
                    ArrayList<Double> throughputInstant = new ArrayList<>();

                    ArrayList<Double> errorGT = new ArrayList<>();
                    LinkedHashMap<String,ArrayList<Double>> errorsGT = new LinkedHashMap<>();

                    ArrayList<Double> errorMLE = new ArrayList<>();
                    LinkedHashMap<String,ArrayList<Double>> errorsMLE = new LinkedHashMap<>();
                    ArrayList<Integer> upViolations = new ArrayList<>();
                    ArrayList<Integer> lowerViolations = new ArrayList<>();

                    ArrayList<Double> runtime = new ArrayList<>();

                    LinkedHashMap<String,LinkedHashMap<String,ArrayList<Double>>> throughputs = new LinkedHashMap<>();
                    LinkedHashMap<String,LinkedHashMap<String,ArrayList<Double>>> numTuples = new LinkedHashMap<>();
                    LinkedHashMap<String,LinkedHashMap<String,ArrayList<Double>>> processingTime = new LinkedHashMap<>();

                    // For each run
                    for (String run : runs) {

                        if (type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                        // Create the input file
                        String newInputFile;
                        if(type.equals("EXACT")){
                            if(runs.size() > 1 ) newInputFile = inputFile+run+"\\"+type+"\\"+exp+"\\communication_cost";
                            else newInputFile = inputFile+type+"\\"+exp+"\\communication_cost";
                        }
                        else{ newInputFile = inputFile+run+"\\"+type+"\\"+error+"\\"+exp+"\\communication_cost"; }
                        // String newInputFile = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\five_runs_epsilon_500K\\run_0\\DETERMINISTIC\\BASELINE\\eps=0.02\\communication_cost";
                        System.out.println("Path : "+newInputFile);

                        // Skip file if does not exist
                        try {
                            FileReader fileReader = new FileReader(newInputFile);
                            fileReader.close();
                        }
                        catch (java.io.FileNotFoundException e) {
                            continue;
                        }

                        // Read file line by line and get the results
                        BufferedReader br = new BufferedReader(new FileReader(newInputFile));
                        String line = br.readLine();
                        boolean findGT = false;
                        boolean findDown = false;
                        boolean findTotal = false;
                        boolean findThroughput = false;

                        int numWorkers = 0;
                        boolean findProcessingTime = false;

                        while (line != null) {

                            line = line.replaceAll("\0", "");

                            // Skip new and empty lines
                            if (line.equals("\n") || line.equals("")) {
                                line = br.readLine();
                                continue;
                            }


                            // Get upstream cost
                            if (line.contains("Size of upstream")) {
                                upCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                            }

                            // Get downstream cost
                            if (line.contains("Size of downstream") && !findDown) {
                                downCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                                findDown = true;
                            }

                            // Get communication cost
                            if (line.contains("Total size") && !findTotal) {
                                totalCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                                findTotal = true;
                            }


                            // Get error to ground truth
                            if (line.contains("Average absolute error :") && !findGT) {
                                errorGT.add(Double.parseDouble(line.split(" : ")[1].trim()));
                                findGT = true;
                                line = br.readLine();
                                continue;
                            }

                            // Get absolute errors to ground truth
                            if (line.contains("Errors to GT :")) {

                                List<Double> errors = Stream.of(line.split(" : ")[1].trim().split(",")).map(Double::valueOf).collect(Collectors.toList());
                                for(int i=0;i<errors.size();i++){ errors.set(i,Math.abs(errors.get(i))); }

                                errorsGT.put(run, new ArrayList<>(errors));
                            }


                            // Get error to EXACT MLE
                            if (line.contains("Average absolute error :") && findGT) {
                                errorMLE.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            }

                            // Get absolute errors to EXACT MLE
                            if (line.contains("Errors to EXACTMLE :")) {

                                List<Double> errors = Stream.of(line.split(" : ")[1].trim().split(",")).map(Double::valueOf).collect(Collectors.toList());
                                for(int i=0;i<errors.size();i++){ errors.set(i,Math.abs(errors.get(i))); }

                                errorsMLE.put(run, new ArrayList<>(errors));
                            }

                            // Get upper and lower violations
                            if (line.contains("Average error :") && line.contains("lower violations :")){
                                String[] violations = line.split(" , ");
                                lowerViolations.add(Integer.parseInt(violations[1].split(" : ")[1].trim()));
                                upViolations.add(Integer.parseInt(violations[2].split(" : ")[1].trim()));
                            }


                            // Get the runtime
                            if (line.contains("Runtime :")) {
                                runtime.add(Double.parseDouble(line.split(" : ")[1].trim().split(" ")[0].trim()));
                            }


                            // Get the throughput(first-way)
                            if (line.contains("Final throughput :") && !findThroughput) {
                                throughputFirst.add(Double.parseDouble(line.split(" : ")[1].trim()));
                                findThroughput = true;
                                line = br.readLine();
                                continue;
                            }

                            // Get the throughput(second-way)
                            if (line.contains("Final throughput :") && findThroughput) {
                                throughputSecond.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            }

                            // Get the throughput(third-way)
                            if (line.contains("Final throughput(Third way) :")) {
                                throughputThird.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            }

                            // Get the instant throughput
                            if (line.contains("Final throughput Instant :")) {
                                throughputInstant.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            }


                            // Throughput,Tuples and Processing Time
                            if( (typeExp.contains("workers") || typeExp.contains("parallelism")) && enableWorkersThroughputs){
                                if (line.contains(" Tuples : ")) {

                                    List<Double> tupleWorker = Stream.of(line.split(" : ")[1].trim().replace("[","").replace("]","").split(", ")).map(Double::valueOf).collect(Collectors.toList());
                                    LinkedHashMap<String,ArrayList<Double>> tuples;
                                    if(numTuples.get(run) == null){
                                        tuples = new LinkedHashMap<>();
                                        tuples.put(String.valueOf(numWorkers),new ArrayList<>(tupleWorker));
                                        numTuples.put(run,tuples);
                                    }
                                    else{
                                        tuples = numTuples.get(run);
                                        tuples.put(String.valueOf(numWorkers), new ArrayList<>(tupleWorker));
                                        numTuples.put(run,tuples);
                                    }
                                }

                                if (line.contains(" Processing time : ")) {

                                    List<Double> timerWorker = Stream.of(line.split(" : ")[1].trim().replace("[","").replace("]","").split(", ")).map(Double::valueOf).collect(Collectors.toList());
                                    LinkedHashMap<String,ArrayList<Double>> timers;
                                    if(processingTime.get(run) == null){
                                        timers = new LinkedHashMap<>();
                                        timers.put(String.valueOf(numWorkers),new ArrayList<>(timerWorker));
                                        processingTime.put(run,timers);
                                    }
                                    else{
                                        timers = processingTime.get(run);
                                        timers.put(String.valueOf(numWorkers), new ArrayList<>(timerWorker));
                                        processingTime.put(run,timers);
                                    }
                                    findProcessingTime = true;
                                }

                                if (line.contains(" Throughput : ") && findProcessingTime ) {

                                    List<Double> throughputWorker = Stream.of(line.split(" : ")[1].trim().replace("[","").replace("]","").split(", ")).map(Double::valueOf).collect(Collectors.toList());
                                    LinkedHashMap<String,ArrayList<Double>> throughputsWorkers;
                                    if(throughputs.get(run) == null){
                                        throughputsWorkers = new LinkedHashMap<>();
                                        throughputsWorkers.put(String.valueOf(numWorkers),new ArrayList<>(throughputWorker));
                                        throughputs.put(run,throughputsWorkers);
                                    }
                                    else{
                                        throughputsWorkers = throughputs.get(run);
                                        throughputsWorkers.put(String.valueOf(numWorkers), new ArrayList<>(throughputWorker));
                                        throughputs.put(run,throughputsWorkers);
                                    }
                                    numWorkers++;
                                }

                            }


                            // Read the next line
                            line = br.readLine();

                        }

                        // Close buffer
                        br.close();
                    }

                    // Write the result to output
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + type + "_" + error + "_" + exp));


                    // Communication Cost
                    writer.write("Size of upstream(bytes) : " + upCommunicationCost + "\n");
                    double upCost = upCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d);
                    writer.write("Average size of upstream(bytes) of runs : " + upCost + "\n\n");

                    writer.write("Size of downstream(bytes) : " + downCommunicationCost + "\n");
                    double downCost = downCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d);
                    writer.write("Average size of downstream(bytes) of runs : " + downCost + "\n\n");

                    writer.write("Total Communication cost(bytes) : " + totalCommunicationCost + "\n");
                    writer.write("Average total communication cost(bytes) of runs : " + totalCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d) + "\n\n\n");
                    // writer.write("Second way average total communication cost(bytes) of five runs : " + (downCost+upCost) + "\n\n");


                    // Error to GT
                    writer.write("Average absolute error(GT) : " + errorGT + "\n");
                    writer.write("Average absolute error(GT) of runs : " + errorGT.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                    if(errorGT.size() == runs.size()) {
                        ArrayList<Double> medianErrorsGT = new ArrayList<>();
                        for (int i = 0; i < errorsGT.get(runs.get(0)).size(); i++) {
                            double sum = 0;
                            for (String run : runs) {
                                sum += errorsGT.get(run).get(i);
                            }
                            medianErrorsGT.add(sum / runs.size());
                        }
                        String str = medianErrorsGT.toString().replace("[", "").replace("]", "");
                        writer.write("Average median absolute error(GT) of runs : " + str + "\n\n\n");
                    }
                    else{ writer.write("Average median absolute error(GT) of runs : -1.0"+"\n\n\n"); }


                    // Error to EXACTMLE
                    writer.write("Average absolute error(EXACTMLE) : " + errorMLE + "\n");
                    writer.write("Average absolute error(EXACTMLE) of runs : " + errorMLE.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                    if(errorMLE.size() == runs.size()) {
                        ArrayList<Double> medianErrorsMLE = new ArrayList<>();
                        for (int i = 0; i < errorsMLE.get(runs.get(0)).size(); i++) {
                            double sum = 0;
                            for (String run : runs) {
                                sum += errorsMLE.get(run).get(i);
                            }
                            medianErrorsMLE.add(sum / runs.size());
                        }
                        String str = medianErrorsMLE.toString().replace("[", "").replace("]", "");
                        writer.write("Average median absolute error(EXACTMLE) of runs : " + str + "\n\n");

                        writer.write("Lower violations(EXACTMLE) : " + lowerViolations + "\n");
                        writer.write("Average of lower violations of runs : " + lowerViolations.stream().mapToDouble(Integer::intValue).average().orElse(-1d) + "\n\n");
                        writer.write("Upper violations(EXACTMLE) : " + upViolations + "\n");
                        writer.write("Average of upper violations of runs : " + upViolations.stream().mapToDouble(Integer::intValue).average().orElse(-1d) + "\n\n\n");
                    }
                    else{
                        writer.write("Average median absolute error(EXACTMLE) of runs : -1.0" + "\n\n\n");
                        writer.write("Lower violations(EXACTMLE) : []" + "\n");
                        writer.write("Average of lower violations of runs : -1.0" + "\n\n");
                        writer.write("Upper violations(EXACTMLE) : []" + "\n");
                        writer.write("Average of upper violations of runs : -1.0" + "\n\n\n");
                    }


                    // Runtime
                    writer.write("Runtime(sec) : " + runtime + "\n");
                    writer.write("Average runtime(sec) of five runs : " + runtime.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n\n");


                    // Throughput
                    writer.write("First-way Throughput(events/sec) : " + throughputFirst + "\n");
                    writer.write("Average first-way throughput(events/sec) of runs : " + throughputFirst.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                    writer.write("Second-way Throughput(events/sec) : " + throughputSecond + "\n");
                    writer.write("Average second-way throughput(events/sec) of runs : " + throughputSecond.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                    writer.write("Third-way Throughput(events/sec) : " + throughputThird + "\n");
                    writer.write("Average third-way throughput(events/sec) of runs : " + throughputThird.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                    writer.write("Instant Throughput(events/sec) : " + throughputInstant + "\n");
                    writer.write("Average instant throughput(events/sec) of runs : " + throughputInstant.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");


                    // Throughput,Tuples and Processing Time
                    if( (typeExp.contains("workers") || typeExp.contains("parallelism")) && enableWorkersThroughputs){

                        int numWorkers;
                        if(typeExp.contains("parallelism")){ numWorkers = 16; }
                        else{
                            // Workers
                            numWorkers = Integer.parseInt(exp.split("=")[1]);
                        }

                        if(numTuples.get("run_0") != null){
                            for(int i=0;i<numWorkers;i++){

                                // Number of tuples
                                ArrayList<Double> tuples = new ArrayList<>();
                                double sumTuples;

                                // Processing Time
                                ArrayList<Double> procTime = new ArrayList<>();
                                double sumTime;

                                // Throughput
                                ArrayList<Double> throughput = new ArrayList<>();
                                double sumThroughput;

                                // Find the minimum size
                                int min = Integer.MAX_VALUE;
                                for (String run : runs) {
                                    if(numTuples.get(run).get(String.valueOf(i)).size() <= min){
                                        min = numTuples.get(run).get(String.valueOf(i)).size();
                                    }
                                }

                                for(int j=0;j<min;j++){
                                    sumTuples = 0d;sumTime=0d;sumThroughput=0d;
                                    for (String run : runs) {
                                        sumTuples += numTuples.get(run).get(String.valueOf(i)).get(j);
                                        sumTime += processingTime.get(run).get(String.valueOf(i)).get(j);
                                        sumThroughput += throughputs.get(run).get(String.valueOf(i)).get(j);
                                    }
                                    tuples.add(sumTuples/runs.size());
                                    procTime.add(sumTime/runs.size());
                                    throughput.add(sumThroughput/runs.size());
                                }

                                // Write the results
                                writer.write("w"+i+"_T = " + tuples + "\n");
                                writer.write("t"+i+" = " + procTime + "\n");
                                writer.write("w"+i+" = " + throughput + "\n");
                                writer.write("Size : " + tuples.size() + "\n\n");
                            }
                        }
                    }


                    // Close the buffer
                    writer.close();
                }
            }
        }

    }

    // Collect stats from one run
    public static void collectStatsRun(String inputFile,String outputFile,String typeExp) throws IOException {


        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM","EXACT"};
        // String[] types = {"RC_mod_sc_change_cond=2"};

        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};
        // String[] setup = {"BASELINE"};

        // Type of experiment
        ArrayList<String> experiment = new ArrayList<>();
        if(typeExp.contains("epsilon")){

            String epsilon;
            for(double eps=0.02;eps<0.14;eps+=0.02) {

                if (eps == 0.1d) epsilon = eps + "0";
                else if (eps >= 0.12d) {
                    epsilon = String.valueOf(eps);
                    epsilon = epsilon.substring(0, 4);
                }
                else epsilon = String.valueOf(eps);
                experiment.add("eps="+epsilon);
            }
        }
        else if(typeExp.contains("workers")){

            // For each worker
            int flag;
            for(int worker=2;worker<=18;worker+=2) {

                flag = worker;
                if (worker == 14) { worker = 16; }
                else if (worker == 16) { worker = 20; }
                else if(worker == 18){ worker = 26; }

                experiment.add("workers="+worker);

                // Reset worker
                worker=flag;
            }
        }
        else if(typeExp.contains("dataset")){
            experiment.add("5K");
            experiment.add("50K");
            experiment.add("500K");
            experiment.add("5M");
        }

        // For each type
        for(String type : types){

            if(type.equals("EXACT")){
                // Modify the array of setup
                setup = new String[1];
                setup[0] = "";

            }
            // For each error setup
            for (String error: setup) {

                ArrayList<Long> totalCommunicationCost = new ArrayList<>();
                ArrayList<Long> upCommunicationCost = new ArrayList<>();
                ArrayList<Long> downCommunicationCost = new ArrayList<>();
                ArrayList<Double> throughputFirst = new ArrayList<>();
                ArrayList<Double> throughputSecond = new ArrayList<>();
                ArrayList<Double> throughputThird = new ArrayList<>();
                ArrayList<Double> throughputInstant = new ArrayList<>();
                ArrayList<Double> errorGT = new ArrayList<>();
                ArrayList<Double> errorMLE = new ArrayList<>();
                ArrayList<Double> runtime = new ArrayList<>();
                ArrayList<String> violations = new ArrayList<>();

                // For each value of experiment
                for ( String exp : experiment) {

                    if (type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                    // Create the input file
                    String newInputFile = inputFile+type+"\\"+error+"\\"+exp+"\\communication_cost";
                    // String newInputFile = inputFile+type+"\\"+error+"\\"+exp+"\\result\\communication_cost";
                    // String newInputFile = "C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\hepar2\\Communication cost-Number of sites_cluster\\five_runs_epsilon_500K\\run_0\\DETERMINISTIC\\BASELINE\\eps=0.02\\communication_cost";

                    // Skip file if does not exist
                    try {
                        FileReader fileReader = new FileReader(newInputFile);
                        fileReader.close();
                    }
                    catch (java.io.FileNotFoundException e) {
                        continue;
                    }

                    // Read file line by line and get the results
                    BufferedReader br = new BufferedReader(new FileReader(newInputFile));
                    String line = br.readLine();
                    boolean findGT = false;
                    boolean findDown = false;
                    boolean findTotal = false;
                    boolean findThroughput = false;

                    while (line != null) {

                        line = line.replaceAll("\0", "");

                        // Skip new and empty lines
                        if (line.equals("\n") || line.equals("")) {
                            line = br.readLine();
                            continue;
                        }

                        // Get upstream cost
                        if (line.contains("Size of upstream")) {
                            upCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                        }

                        // Get downstream cost
                        if (line.contains("Size of downstream") && !findDown) {
                            downCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                            findDown = true;
                        }

                        // Get communication cost
                        if (line.contains("Total size") && !findTotal) {
                            totalCommunicationCost.add(Long.parseLong(line.split(" : ")[1].trim()));
                            findTotal = true;
                        }

                        // Get error to ground truth
                        if (line.contains("Average absolute error :") && !findGT) {
                            errorGT.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            findGT = true;
                            line = br.readLine();
                            continue;
                        }

                        // Get error to EXACT MLE
                        if (line.contains("Average absolute error :") && findGT) {
                            errorMLE.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get the runtime
                        if (line.contains("Runtime :")) {
                            runtime.add(Double.parseDouble(line.split(" : ")[1].trim().split(" ")[0].trim()));
                        }

                        // Get the throughput(first-way)
                        if (line.contains("Final throughput :") && !findThroughput) {
                            throughputFirst.add(Double.parseDouble(line.split(" : ")[1].trim()));
                            findThroughput = true;
                            line = br.readLine();
                            continue;
                        }

                        // Get the throughput(second-way)
                        if (line.contains("Final throughput :") && findThroughput) {
                            throughputSecond.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get lower and upper violations
                        if (line.contains("lower violations :") ) {
                            violations.add(line.trim());
                        }

                        // Get the throughput(third-way)
                        if (line.contains("Final throughput(Third way) :")) {
                            throughputThird.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Get the instant throughput
                        if (line.contains("Final throughput Instant :")) {
                            throughputInstant.add(Double.parseDouble(line.split(" : ")[1].trim()));
                        }

                        // Read the next line
                        line = br.readLine();

                    }

                    // Close buffer
                    br.close();
                }

                // Write the result to output
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + type + "_" + error + "_" + typeExp));

                writer.write("Size of upstream(bytes) : " + upCommunicationCost + "\n");
                double upCost = upCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d);
                writer.write("Average size of upstream(bytes) of one run : " + upCost + "\n\n");

                writer.write("Size of downstream(bytes) : " + downCommunicationCost + "\n");
                double downCost = downCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d);
                writer.write("Average size of downstream(bytes) of five runs : " + downCost + "\n\n");

                writer.write("Total Communication cost(bytes) : " + totalCommunicationCost + "\n");
                writer.write("Average total communication cost(bytes) of five runs : " + totalCommunicationCost.stream().mapToLong(Long::longValue).average().orElse(-1d) + "\n\n");

                writer.write("Average absolute error(GT) : " + errorGT + "\n");
                writer.write("Average absolute error(GT) of five runs : " + errorGT.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Average absolute error(EXACTMLE) : " + errorMLE + "\n");
                writer.write("Average absolute error(EXACTMLE) of five runs : " + errorMLE.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Runtime(sec) : " + runtime + "\n");
                writer.write("Average runtime(sec) of five runs : " + runtime.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("First-way Throughput(events/sec) : " + throughputFirst + "\n");
                writer.write("Average first-way throughput(events/sec) of five runs : " + throughputFirst.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Second-way Throughput(events/sec) : " + throughputSecond + "\n");
                writer.write("Average second-way throughput(events/sec) of five runs : " + throughputSecond.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Third-way Throughput(events/sec) : " + throughputThird + "\n");
                writer.write("Average third-way throughput(events/sec) of  runs : " + throughputThird.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Instant Throughput(events/sec) : " + throughputInstant + "\n");
                writer.write("Average instant throughput(events/sec) of  runs : " + throughputInstant.stream().mapToDouble(Double::doubleValue).average().orElse(-1d) + "\n\n");

                writer.write("Violations  : " + violations + "\n");

                // Close the buffer
                writer.close();
            }
        }

    }


    // Collect stats from parameter workers
    public static void collectWorkers(String inputFile,String outputFile,String exactFile,boolean enableWorkersStats,boolean enableEXACT) throws IOException {

        String[] types;
        if(enableEXACT){ types = new String[]{"EXACT"}; }
        else{
            types = new String[]{"RANDOMIZED","DETERMINISTIC","FGM"};
            // types = new String[]{"RANDOMIZED"};
        }

        // String[] setup = {"BASELINE"};
        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};

        // For each type
        for(String type : types){

            if(type.equals("EXACT")){
                // Modify the array of setup
                setup = new String[1];
                setup[0] = "";

            }

            // For each error setup
            for (String error: setup) {

                if(type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                // For each worker
                int flag;
                for(int worker=2;worker<=66;worker+=2){

                    flag=worker;
                    if (worker == 14) { worker = 16; }
                    else if (worker == 16) { worker = 20; }
                    else if(worker == 18){ worker = 26; }

                    // Workers >= 26 , numWorkers = 66
                    // if( worker != 32 && worker != 38 && worker != 44 && worker != 52 && worker != 60 && worker != 64 ){ continue; }

                    String newInputFile;
                    if(type.equals("EXACT")){ newInputFile = inputFile+type+"\\workers="+worker; }
                    else{ newInputFile = inputFile+type+"\\"+error+"\\workers="+worker; }
                    System.out.println("Path : "+newInputFile);
                    // String newInputFile = inputFile+type+"\\"+error+"\\workers="+worker+"\\result";

                    String newExactFile = exactFile+"EXACT"+"\\workers="+worker;
                    // String newExactFile = exactFile+"EXACT"+"\\workers=16";
                    // String newExactFile = inputFile+"EXACT";

                    // Skip file if does not exist
                    try{
                        String testNewInputFile = newInputFile+"\\coordinator_stats.txt";
                        FileReader fileReader = new FileReader(testNewInputFile);
                        fileReader.close();
                    }
                    catch(java.io.FileNotFoundException e){ continue; }

                    // Clear statistics file if exist
                    BufferedWriter writer = new BufferedWriter(new FileWriter(newInputFile+"\\communication_cost",false));
                    writer.close();

                    if(type.equals("FGM")){ collectMessagesFGMFile(newInputFile,newInputFile,22550); }
                    else{
                        if(type.equals("DETERMINISTIC")) collectMessagesFile(newInputFile,newInputFile,worker,"DC");
                        else collectMessagesFile(newInputFile,newInputFile,worker,"RC");
                    }

                    getErrorEXACTFile(newInputFile,newExactFile,newInputFile,0.1d);
                    getRuntimeFile(newInputFile,newInputFile);
                    if(enableWorkersStats) {
                        getThroughputFile(newInputFile, newInputFile, worker);
                        getTuplesProcFile(newInputFile, newInputFile, worker);
                        getTuplesFile(newInputFile, newInputFile, worker);
                    }

                    // Reset worker
                    worker=flag;
                }
            }

        }
    }

    // Collect stats from parameter workers for runs
    public static void collectWorkersRuns(String inputFile,String outputFile,String exactFile,int numOfRuns,boolean enableWorkersStats) throws IOException{

        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        // For each run
        for (String run: runs){
            String file = inputFile+run+"\\";
            // String file1 = exactFile+run+"\\"; as exactFile
            collectWorkers(file,file,exactFile,enableWorkersStats,false);
        }

        // EXACT
        collectWorkers(inputFile,inputFile,exactFile,enableWorkersStats,true);
        // For each run
        /*for (String run: runs){
            String file = inputFile+run+"\\";
            collectWorkers(file,file,file,enableWorkersStats,true);
        }*/

    }


    // Collect stats from parameter epsilon
    public static void collectEpsilon(String inputFile,String outputFile,String exactFile,int workers,boolean enableWorkersStats) throws IOException {


        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM"};
        // String[] types = {"RANDOMIZED"};

         String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};
        // String[] setup = {"BASELINE"};

        // For each type
        for(String type : types){
            // For each error setup
            for (String error: setup) {

                if(type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                // For each worker
                String epsilon;
                for(double eps=0.02;eps<0.14;eps+=0.02){

                    if(eps == 0.1d) epsilon = eps + "0";
                    else if(eps >= 0.12d){
                        epsilon = String.valueOf(eps);
                        epsilon = epsilon.substring(0,4);
                    }
                    else epsilon = String.valueOf(eps);

                    String newInputFile = inputFile+type+"\\"+error+"\\eps="+epsilon;
                    System.out.println("Path : "+newInputFile);

                    // String newExactFile = inputFile+"EXACT";
                    String newExactFile = exactFile;

                    // Skip file if does not exist
                    try{
                        String testNewInputFile = newInputFile+"\\coordinator_stats.txt";
                        FileReader fileReader = new FileReader(testNewInputFile);
                        fileReader.close();
                    }
                    catch(java.io.FileNotFoundException e){ continue; }

                    // Clear statistics file if exist
                    BufferedWriter writer = new BufferedWriter(new FileWriter(newInputFile+"\\communication_cost",false));
                    writer.close();

                    if(type.equals("FGM")){ collectMessagesFGMFile(newInputFile,newInputFile,22550); }
                    else{
                        if(type.equals("DETERMINISTIC")) collectMessagesFile(newInputFile,newInputFile,workers,"DC");
                        else collectMessagesFile(newInputFile,newInputFile,workers,"RC");
                    }

                    getErrorEXACTFile(newInputFile,newExactFile,newInputFile,eps);
                    getRuntimeFile(newInputFile,newInputFile);
                    if(enableWorkersStats){
                        getThroughputFile(newInputFile,newInputFile,workers);
                        getTuplesProcFile(newInputFile,newInputFile,workers);
                        getTuplesFile(newInputFile,newInputFile,workers);
                    }

                }
            }

        }

        // EXACT
        // Clear statistics file if exist
        BufferedWriter writer = new BufferedWriter(new FileWriter(exactFile+"\\communication_cost",false));
        writer.close();

        collectMessagesFile(exactFile,exactFile,workers,"RC");
        getErrorEXACTFile(exactFile,exactFile,exactFile,0.1);
        getRuntimeFile(exactFile,exactFile);
        if(enableWorkersStats){
            getThroughputFile(exactFile,exactFile,workers);
            getTuplesProcFile(exactFile,exactFile,workers);
            getTuplesFile(exactFile,exactFile,workers);
        }

    }

    // Collect stats from parameter epsilon for runs
    public static void collectEpsilonRuns(String inputFile,String outputFile,String exactFile,int numOfRuns,int workers,boolean enableWorkersStats) throws IOException{

        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        // For each run
        for (String run: runs){
            String file = inputFile+run+"\\";
            collectEpsilon(file,file,exactFile,workers,enableWorkersStats);
        }

    }


    // Collect stats from size of datasets
    public static void collectDatasets(String inputFile,String outputFile,String exactFile,int workers,boolean enableWorkersStats,boolean enableEXACT) throws IOException {

        String[] types;
        if(enableEXACT){ types = new String[]{"EXACT"}; }
        else{
            types = new String[]{"RANDOMIZED","DETERMINISTIC","FGM"};
            // types = new String[]{"RANDOMIZED"};
        }


        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};
        // String[] setup = {"BASELINE"};

        String[] sizeOfDatasets = {"5K","50K","500K","5M","50M"};
        // String[] sizeOfDatasets = {"5K"};

        // For each type
        for(String type : types){

            if(type.equals("EXACT")){
                // Modify the array of setup
                setup = new String[1];
                setup[0] = "";

            }

            // For each error setup
            for (String error: setup) {

                if(type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                // For each worker
                for(String dataset : sizeOfDatasets){

                    String newInputFile;
                    if(type.equals("EXACT")){ newInputFile = inputFile+type+"\\"+dataset; }
                    else{ newInputFile = inputFile+type+"\\"+error+"\\"+dataset; }
                    System.out.println("Path : "+newInputFile);

                    // String newExactFile = inputFile+"EXACT"+"\\"+dataset;
                    String newExactFile = exactFile+"EXACT"+"\\"+dataset;

                    // Skip file if does not exist
                    try{
                        String testNewInputFile = newInputFile+"\\coordinator_stats.txt";
                        FileReader fileReader = new FileReader(testNewInputFile);
                        fileReader.close();
                    }
                    catch(java.io.FileNotFoundException e){ continue; }

                    // Clear statistics file if exist
                    BufferedWriter writer = new BufferedWriter(new FileWriter(newInputFile+"\\communication_cost",false));
                    writer.close();

                    // HEPAR2 -> vectorSize message 22550
                    // ALARM -> vectorSize message 7886
                    // LINK -> vectorSize message 212894
                    if(type.equals("FGM")){ collectMessagesFGMFile(newInputFile,newInputFile,212894); }
                    else{
                        if(type.equals("DETERMINISTIC")) collectMessagesFile(newInputFile,newInputFile,workers,"DC");
                        else collectMessagesFile(newInputFile,newInputFile,workers,"RC");
                    }

                    getErrorEXACTFile(newInputFile,newExactFile,newInputFile,0.1);
                    getRuntimeFile(newInputFile,newInputFile);
                    if(enableWorkersStats){
                        getThroughputFile(newInputFile,newInputFile,workers);
                        getTuplesProcFile(newInputFile,newInputFile,workers);
                        getTuplesFile(newInputFile,newInputFile,workers);
                    }

                }
            }

        }

    }

    // Collect stats from parameter epsilon for runs
    public static void collectDatasetRuns(String inputFile,String outputFile,String exactFile,int numOfRuns,int workers,boolean enableWorkersStats) throws IOException{

        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        // For each run
        for (String run: runs){
            String file = inputFile+run+"\\";
            collectDatasets(file,file,exactFile,workers,enableWorkersStats,false);
        }

        // EXACT
        collectDatasets(inputFile,inputFile,exactFile,workers,enableWorkersStats,true);

    }


    // Collect stats from parameter parallelism
    public static void collectParallelism(String inputFile,String outputFile,String exactFile,int workers,boolean enableWorkersStats) throws IOException {


        String[] types = {"RANDOMIZED","DETERMINISTIC","FGM"};
        String[] setup = {"BASELINE","UNIFORM","NON_UNIFORM"};

        // For each type
        for(String type : types){

            // For each error setup
            for (String error: setup) {

                if(type.equals("FGM") && error.equals("NON_UNIFORM")) continue;

                // For each worker
                for(int parallelism=1;parallelism<=16;parallelism+=1){

                    // Step=1
                    // if( (parallelism % 2) != 0 && (parallelism % 3) != 0 ){ continue; }
                    // if( (parallelism > 12 && parallelism<=15)){ continue; }

                    // Parallelism = 44,step=1
                    // if(parallelism != 3 && parallelism != 6 && parallelism != 8 && parallelism != 10 && parallelism != 12 && parallelism != 42 ) { continue; }

                    // Parallelism = 15,step=1
                    // if(parallelism != 1 && parallelism != 2 && parallelism != 3 && parallelism != 4 && parallelism != 6 && parallelism != 8 && parallelism != 10 && parallelism != 12 && parallelism != 15){ continue; }

                    // Parallelism = 16,step=1
                    if( parallelism != 1 && parallelism != 2 && parallelism != 3 && parallelism != 4 && parallelism != 6 && parallelism != 7 && parallelism != 9 && parallelism != 10 && parallelism != 12 && parallelism != 14 &&  parallelism != 16){ continue; }

                    String newInputFile = inputFile+type+"\\"+error+"\\parallelism="+parallelism;
                    System.out.println("Path : "+newInputFile);

                    // String newExactFile = inputFile+"EXACT"+"\\parallelism="+parallelism;
                    String newExactFile = exactFile+"EXACT"+"\\parallelism="+parallelism;

                    // Skip file if does not exist
                    try{
                        String testNewInputFile = newInputFile+"\\coordinator_stats.txt";
                        FileReader fileReader = new FileReader(testNewInputFile);
                        fileReader.close();
                    }
                    catch(java.io.FileNotFoundException e){ continue; }

                    // Clear statistics file if exist
                    BufferedWriter writer = new BufferedWriter(new FileWriter(newInputFile+"\\communication_cost",false));
                    writer.close();

                    if(type.equals("FGM")){ collectMessagesFGMFile(newInputFile,newInputFile,22550); }
                    else{
                        if(type.equals("DETERMINISTIC")) collectMessagesFile(newInputFile,newInputFile,workers,"DC");
                        else collectMessagesFile(newInputFile,newInputFile,workers,"RC");
                    }

                    getErrorEXACTFile(newInputFile,newExactFile,newInputFile,0.1d);
                    getRuntimeFile(newInputFile,newInputFile);
                    if(enableWorkersStats){
                        getThroughputFile(newInputFile,newInputFile,workers);
                        getTuplesProcFile(newInputFile,newInputFile,workers);
                        getTuplesFile(newInputFile,newInputFile,workers);
                    }

                }
            }
        }


        // EXACT
        // For each worker
        for(int parallelism=1;parallelism<=16;parallelism+=1){

            // Step=1
            // if( (parallelism % 2) != 0 && (parallelism % 3) != 0 ){ continue; }
            // if( (parallelism > 12 && parallelism<=15)){ continue; }

            // Parallelism = 44,step=1
            // if(parallelism != 3 && parallelism != 6 && parallelism != 8 && parallelism != 10 && parallelism != 12 && parallelism != 42 ) { continue; }

            // Parallelism = 15,step=1
            // if(parallelism != 1 && parallelism != 2 && parallelism != 3 && parallelism != 4 && parallelism != 6 && parallelism != 8 && parallelism != 10 && parallelism != 12 && parallelism != 15){ continue; }

            // Parallelism = 16,step=1
            if( parallelism != 1 && parallelism != 2 && parallelism != 3 && parallelism != 4 && parallelism != 6 && parallelism != 7 && parallelism != 9 && parallelism != 10 && parallelism != 12 && parallelism != 14 &&  parallelism != 16){ continue; }

            String newExactFile = inputFile+"EXACT"+"\\parallelism="+parallelism;
            System.out.println("Path : "+newExactFile);

            // Skip file if does not exist
            try{
                String testNewInputFile = newExactFile+"\\coordinator_stats.txt";
                FileReader fileReader = new FileReader(testNewInputFile);
                fileReader.close();
            }
            catch(java.io.FileNotFoundException e){ continue; }

            // Clear statistics file if exist
            BufferedWriter writer = new BufferedWriter(new FileWriter(newExactFile+"\\communication_cost",false));
            writer.close();

            collectMessagesFile(newExactFile,newExactFile,workers,"RC");
            getErrorEXACTFile(newExactFile,newExactFile,newExactFile,0.1d);
            getRuntimeFile(newExactFile,newExactFile);
            getThroughputFile(newExactFile,newExactFile,workers);
            getTuplesProcFile(newExactFile,newExactFile,workers);
            getTuplesFile(newExactFile,newExactFile,workers);
        }

    }

    // Collect stats from parameter parallelism for number of runs
    public static void collectParallelismRuns(String inputFile,String outputFile,String exactFile,int numOfRuns,int workers,boolean enableWorkersStats) throws IOException{

        ArrayList<String> runs = new ArrayList<>();
        for(int i=0;i<numOfRuns;i++){ runs.add("run_"+i); }

        // For each run
        for (String run: runs){
            String file = inputFile+run+"\\";
            String file1 = exactFile+run+"\\";
            collectParallelism(file,file,file1,workers,enableWorkersStats);
        }

    }


    // Collect the messages from workers and coordinator and write the results to file
    public static void collectMessagesFile(String inputFile,String outputFile,int workers,String typeCounter) throws IOException {


        long totalMessagesWorker = 0L;
        long totalMessagesCoordinator = 0L;

        // Collect number of messages for all workers
        for(int i=0;i<workers;i++){
            System.out.println("File : " + "worker" + i + ".messages");
            if( typeCounter.equals("RC") || typeCounter.equals("RANDOMIZED") ){
                try{
                    String workerFile = inputFile+"\\worker_stats_"+i+".txt";
                    FileReader fileReader = new FileReader(workerFile);
                    fileReader.close();
                }
                catch(java.io.FileNotFoundException e){ continue; }
                totalMessagesWorker += collectMessagesRCFile(inputFile+"\\worker_stats_"+i+".txt",outputFile,i);
            }
            else{
                try{
                    String workerFile = inputFile+"\\worker_stats_"+i+".txt";
                    FileReader fileReader = new FileReader(workerFile);
                    fileReader.close();
                }
                catch(java.io.FileNotFoundException e){ continue; }
                totalMessagesWorker += collectMessagesDCFile(inputFile+"\\worker_stats_"+i+".txt",outputFile,i);
            }

        }

        // Collect number of messages for coordinator
        System.out.println("File : coordinator_messages");
        if( typeCounter.equals("RC") || typeCounter.equals("RANDOMIZED") ){
            totalMessagesCoordinator += collectMessagesRCFile(inputFile+"\\coordinator_stats.txt",outputFile,-1);
        }
        else{
            totalMessagesCoordinator += collectMessagesDCFile(inputFile+"\\coordinator_stats.txt",outputFile,-1);
        }


        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        System.out.println("Total messages workers : " + totalMessagesWorker);
        writer.write("\nTotal messages workers : " + totalMessagesWorker+"\n");

        System.out.println("Size of downstream : " + (totalMessagesWorker*31)+"\n");
        writer.write("Size of downstream : " + (totalMessagesWorker*31)+"\n");

        System.out.println("Total messages coordinator : " + totalMessagesCoordinator);
        // writer.write("Total messages coordinator : " + totalMessagesCoordinator+"\n");

        System.out.println("Total size(bytes) : " + ((totalMessagesWorker*31)+(totalMessagesCoordinator*31)));
        writer.write("Total size(bytes) : " + ((totalMessagesWorker*31)+(totalMessagesCoordinator*31))+"\n");

        // Close the buffer
        writer.close();
    }

    // Collect the messages of RC counters and write the results to file
    public static long collectMessagesRCFile(String file,String outputFile,int worker) throws IOException {
        
        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));
        if(worker == -1){ writer.write("\nFile : coordinator_messages"+"\n"); }
        else{
            // Get the number of worker
            writer.write("\nFile : " + "worker" + worker + ".messages"+"\n");
        }


        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long numMessagesSentWorkers = 0; // Number of messages sent from Workers
        long overheadMessages = 0; // overhead of messages to Coordinator
        int zeroCountersCord = 0; // Counter with zero value at Coordinator
        int counterProb = 0; // The total number of counters with probability=1
        // For coordinator file the numMessages = num_round of each Counter - RANDOMIZED case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split("=>")[1].trim().replace(" : ",":").split(" ");

                numMessagesSent = Long.parseLong(splits[0].split(":")[1].trim());
                overheadMessages = Long.parseLong(splits[1].split(":")[1].trim());
                if(splits.length > 2) numMessagesSentWorkers = Long.parseLong(splits[2].split(":")[1].trim());
            }

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("End of process")){
                find = true;
                line = br.readLine();
                continue;
            }

            if(find && line.contains("Counter")){

                if(line.contains("Coordinator state") || line.contains("Worker state ")){
                    line = br.readLine();
                    continue;
                }

                // Process the counter

                // Split the lines based on colon
                Counter counter = strToCounter(line);

                if(counter.getNumMessages() == 0L) messagesZero++;
                if(counter.getCounter() == 0L) zeroCounters++;
                if(counter.getLastSentValues() != null) {
                    int zero = 0;
                    for (Long value : counter.getLastSentValues().values()) { if (value == 0L) zero++; }
                    if(zero == counter.getLastSentValues().size()) zeroCountersCord++;
                }
                if(counter.getProb() == 1d) counterProb++;

                // Update the messages
                messages += counter.getNumMessages();

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            String x = "Total counters : " + totalCount
                        + " , Total number of rounds for all counters : " + messages
                        + " , Total number of counters with zero rounds : " + messagesZero
                        + " , Zero counters : " + zeroCountersCord
                        + " , numMessagesSent : " + numMessagesSent
                        + " , overheadMessages : " + overheadMessages
                        + " , numMessagesSentWorkers : " + numMessagesSentWorkers
                        + " , Total number of counters with prob=1 : " + counterProb;

            System.out.println(x);
            writer.write(x+"\n");

            System.out.println("Total messages coordinator : "
                                + " numMessagesSent : " + numMessagesSent
                                + " , overheadMessages : " + overheadMessages);
            writer.write("\nTotal messages coordinator : "
                            + " numMessagesSent : " + numMessagesSent
                            + " , overheadMessages : " + overheadMessages);

            System.out.println("Size of upstream : " + numMessagesSent*31 );
            writer.write("\nSize of upstream : " + numMessagesSent*31 );

            System.out.println("Total messages workers : " + numMessagesSentWorkers);
            writer.write("\n\nTotal messages workers : " + numMessagesSentWorkers+"\n");

            System.out.println("Size of downstream : " + numMessagesSentWorkers*31 );
            writer.write("Size of downstream : " + numMessagesSentWorkers*31 );

            System.out.println("Total size(bytes) : " + ((numMessagesSent*31)+(numMessagesSentWorkers*31)));
            writer.write("\nTotal size(bytes) : " + ((numMessagesSent*31)+(numMessagesSentWorkers*31))+"\n");

            writer.close();
            return numMessagesSent;
        }
        // Workers
        else {

            String x = "Total counters : " + totalCount
                        + " , Total messages : " + messages
                        + " , Zero messages : " + messagesZero
                        + ", Zero counters : " + zeroCounters
                        + " , Total number of counters with prob=1 : " + counterProb
                        + " , numMessagesSent : " + numMessagesSent
                        + " , overheadMessages : " + overheadMessages;

            System.out.println(x);
            writer.write(x+"\n");
        }
        
        // Close the buffer
        writer.close();
        br.close();
        return messages;
    }

    // Collect the messages of DC counters and write the results to file
    public static long collectMessagesDCFile(String file,String outputFile,int worker) throws IOException {

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));
        if(worker == -1){ writer.write("\nFile : coordinator_messages"+"\n"); }
        else{
            // Get the number of worker
            writer.write("\nFile : " + "worker" + worker + ".messages"+"\n");
        }

        // First input
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        int totalCount = 0; // Total count of counters
        boolean find = false;
        long messages = 0L; // Number of messages sent by Worker
        int messagesZero = 0; // Counters with zero messages
        int zeroCounters = 0; // Counters with zero value
        long numMessagesSent = 0; // Number of messages sent from Coordinator
        long numMessagesSentWorkers = 0; // Number of messages sent from Workers
        long overheadMessages = 0; // overhead of messages to Coordinator
        long numberRounds = 0; // Number of total rounds from all counters
        int zeroRounds = 0; // Counter with zero rounds at Coordinator
        // For coordinator file the counterDoubles = num_round of each Counter - DETERMINISTIC case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages(valid only for Coordinator)
            if(line.contains("numMessagesSent") && !line.contains("datasetSchema")){

                String[] splits = line.trim().split("=>")[1].trim().replace(" : ",":").split(" ");

                numMessagesSent = Long.parseLong(splits[0].split(":")[1].trim());
                overheadMessages = Long.parseLong(splits[1].split(":")[1].trim());
                if(splits.length > 2) numMessagesSentWorkers = Long.parseLong(splits[2].split(":")[1].trim());
            }

            // Skip the lines , until reach the Coordinator or Worker state
            if(line.contains("End of process")){
                find = true;
                line = br.readLine();
                continue;
            }


            if(find && line.contains("Counter")){

                if(line.contains("Coordinator state") || line.contains("Worker state ")){
                    line = br.readLine();
                    continue;
                }

                // Process the counter

                // Split the lines based on colon
                Counter counter = strToCounter(line);
                if(counter.getNumMessages() == 0L) messagesZero++;
                if(counter.getCounter() == 0L) zeroCounters++;
                numberRounds += counter.getCounterDoubles();
                if(counter.getCounterDoubles() == 0L) zeroRounds++;

                // Update the messages
                messages += counter.getNumMessages();

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        // Coordinator
        if(file.contains("coordinator")){

            String x = "Total counters : " + totalCount
                    + " , Total number of rounds for all counters : " + numberRounds
                    + " , Total number of counters with zero rounds : " + zeroRounds
                    + " , Zero counters : " + zeroCounters
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages
                    + " , numMessagesSentWorkers : " + numMessagesSentWorkers;

            System.out.println(x);
            writer.write(x+"\n");

            System.out.println("Total messages coordinator : "
                                + " numMessagesSent : " + numMessagesSent
                                + " , overheadMessages : " + overheadMessages);
            writer.write("\nTotal messages coordinator : "
                            + " numMessagesSent : " + numMessagesSent
                            + " , overheadMessages : " + overheadMessages);

            System.out.println("Size of upstream : " + numMessagesSent*31 );
            writer.write("\nSize of upstream : " + numMessagesSent*31 );

            System.out.println("Total messages workers : " + numMessagesSentWorkers);
            writer.write("\n\nTotal messages workers : " + numMessagesSentWorkers+"\n");

            System.out.println("Size of downstream : " + numMessagesSentWorkers*31 );
            writer.write("Size of downstream : " + numMessagesSentWorkers*31 );

            System.out.println("Total size(bytes) : " + ((numMessagesSent*31)+(numMessagesSentWorkers*31)));
            writer.write("\nTotal size(bytes) : " + ((numMessagesSent*31)+(numMessagesSentWorkers*31))+"\n");

            writer.close();
            return numMessagesSent;
        }
        // Workers
        else {

            String x = "Total counters : " + totalCount
                    + " , Total messages : " + messages
                    + " , Zero messages : " + messagesZero
                    + ", Zero counters : " + zeroCounters
                    + " , numMessagesSent : " + numMessagesSent
                    + " , overheadMessages : " + overheadMessages;

            System.out.println(x);
            writer.write(x+"\n");
        }

        writer.close();
        br.close();
        return messages;
    }

    // Collect messages from FGM method and write the results to file
    public static void collectMessagesFGMFile(String path,String outputFile,long vectorSize) throws IOException {

        try{
            String file = path+"\\coordinator_stats.txt";
            FileReader fileReader = new FileReader(file);
            fileReader.close();
        }
        catch(java.io.FileNotFoundException e){ return; }

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));
        System.out.println("File : coordinator_stats");
        writer.write("File : coordinator_stats\n");

        // Input stream
        BufferedReader br = new BufferedReader(new FileReader(path+"\\coordinator_stats.txt"));
        String line = br.readLine();

        long totalMessagesWorkers = 0L; // Total number of messages from Workers
        long numSentMes = 0L; // Number of sent messages
        long numDriftMes = 0L; // Number of drift messages
        long numSenEstMes = 0L; // Number of estimation messages
        String cordState = null;

        while ( line != null ) {

            line = line.replaceAll("\0", "");

            // Skip new and empty lines
            if (line.equals("\n") || line.equals("")) {
                line = br.readLine();
                continue;
            }

            // Process logic
            if(line.contains("Coordinator state =>")) {

                // Split the lines based on =>
                String splittedLine = line.split("=>")[1];

                // Update the coordinator state
                cordState = splittedLine.trim();
                System.out.println(splittedLine.trim());

                String[] splits = splittedLine.split(",");

                for (String split : splits) {

                    if (split.contains("numMessagesSent")){
                        numSentMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfSentEstMessages")){
                        numSenEstMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfDriftMessages")){
                        numDriftMes += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfIncrementMessages")){
                        totalMessagesWorkers += Long.parseLong(split.split(":")[1].trim());
                    }
                    if (split.contains("numOfZetaMessages")){
                        totalMessagesWorkers += Long.parseLong(split.split(":")[1].trim());
                    }
                }
            }

            // Read the next line
            line = br.readLine();

        }


        // Write the coordinator state
        writer.write("\n"+cordState+"\n");

        String x =  "Total messages workers : " + totalMessagesWorkers
                    + " , numMessagesSent : " + (numSentMes - numSenEstMes) + "\n"
                    + "Messages of drift-estimate :  " + numSenEstMes + " + " + numDriftMes + " = " + (numSenEstMes + numDriftMes);

        long finalNumSentMes = numSentMes - numSenEstMes;
        System.out.println(x);
        writer.write(x+"\n");

        long totalUpStream = finalNumSentMes+numSenEstMes;
        System.out.println("\nTotal upstream messages : " + totalUpStream);
        writer.write("\nTotal upstream messages : " + totalUpStream);

        long totalSizeUpStream = (finalNumSentMes*22)+(numSenEstMes*vectorSize);
        System.out.println("Size of upstream messages : " + totalSizeUpStream);
        writer.write("\nSize of upstream messages : " + totalSizeUpStream);

        long totalDownStream = totalMessagesWorkers+numDriftMes;
        System.out.println("Total downstream messages : " + totalDownStream);
        writer.write("\n\nTotal downstream messages : " + totalDownStream);

        long totalSizeDownStream = (totalMessagesWorkers*22)+(numDriftMes*vectorSize);
        System.out.println("Size of downstream messages : " + totalSizeDownStream);
        writer.write("\nSize of downstream messages : " + totalSizeDownStream);

        System.out.println("Total messages : " + (totalUpStream+totalDownStream));
        writer.write("\n\nTotal messages : " + (totalUpStream+totalDownStream));
        System.out.println("Total size of messages : " + (totalSizeUpStream+totalSizeDownStream));
        writer.write("\nTotal size of messages : " + (totalSizeUpStream+totalSizeDownStream));

        // Close the buffer
        writer.close();
        br.close();
    }

    // Read file and get the errors to GT as string line
    public static void getErrorGTFile(String inputFile,String outputFile) throws IOException {

        try{
            String file = inputFile+"\\coordinator_stats.txt";
            FileReader fileReader = new FileReader(file);
            fileReader.close();
        }
        catch(java.io.FileNotFoundException e){ return; }

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        // Input stream
        BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\coordinator_stats.txt"));
        String line1 = br1.readLine();

        int querySize = 0;
        double avg_abs_error_truth = 0;
        double avg_error_truth = 0;
        double tmp_error = 0;
        String errors = "";
        String errors_runs = "";
        String errors_runs_200 = "";
        String gt_prob = "";

        while ( line1 != null ) {

            line1 = line1.replaceAll("\0","");
            if( !line1.contains("Input : Input =>") ){ line1 = br1.readLine(); }

            if(  line1 != null && line1.contains("Input : Input =>") ){

                // Process the inputs from the file
                String[] splitsLine = line1.split("=>")[1].trim().split(" , ");
                String error = null;String gtProb = null;

                // For each split
                for(int i=0;i<splitsLine.length;i++){
                    if(splitsLine[i].contains("error") && !splitsLine[i].contains("absolute") ) error =  splitsLine[i].split(":")[1].trim();
                    if(splitsLine[i].contains("truth probability")) gtProb = splitsLine[i].split(":")[1].trim();
                }

                // Get the ground truth probability
                gt_prob = gt_prob.concat(gtProb.trim()+",");

                // Find the error between two lines(estimated-exact)
                double errorTruth = Double.parseDouble(error);
                double absErrorTruth = Math.abs(errorTruth);
                avg_error_truth += errorTruth;
                avg_abs_error_truth += absErrorTruth;

                // Append the error
                errors = errors.concat(errorTruth +",");

                // Read the next lines
                line1 = br1.readLine();
                querySize++;

                // Append the avg error per 200 queries and calculate the avg error per 200 queries
                tmp_error += errorTruth;
                if( querySize != 0 && querySize % 200 == 0 ){
                    tmp_error = tmp_error/200;
                    errors_runs = errors_runs.concat(tmp_error+",");
                    errors_runs_200 = errors_runs_200.concat((avg_error_truth / querySize) +",");
                    tmp_error = 0;
                }
            }

        }

        System.out.println("Error to ground truth");
        writer.write("\n\nError to ground truth\n");

        String s = "Ground truth probability : " + gt_prob.substring(0, gt_prob.length() - 1);
        System.out.println(s);
        writer.write(s+"\n");

        String s1 = "\n\nErrors to GT : " + errors.substring(0, errors.length() - 1);
        System.out.println(s1);
        writer.write(s1+"\n");

        String s2 = "Errors runs : " + errors_runs.substring(0, errors_runs.length() - 1);
        System.out.println(s2);
        writer.write(s2+"\n");

        String s3 = "Errors runs 200 : " + errors_runs_200.substring(0, errors_runs_200.length() - 1);
        System.out.println(s3);
        writer.write(s3+"\n");

        System.out.println( "Average absolute error : " + avg_abs_error_truth/querySize);
        writer.write( "Average absolute error : " + avg_abs_error_truth/querySize+"\n");

        System.out.println( "Average error : " + avg_error_truth/querySize);
        writer.write( "Average error : " + avg_error_truth/querySize+"\n");

        System.out.println("Queries size : " + querySize);
        writer.write("Queries size : " + querySize+"\n");

        System.out.println("End!!!");
        writer.write("End!!!"+"\n");

        // Close the buffer
        writer.close();
        br1.close();
    }

    // Read file and get the errors to EXACTMLE as string line
    public static void getErrorEXACTFile(String inputFile1,String inputFile2,String outputFile,double eps) throws IOException{

        // InputFile1
        try{
            String file = inputFile1+"\\coordinator_stats.txt";
            FileReader fileReader = new FileReader(file);
            fileReader.close();
        }
        catch(java.io.FileNotFoundException e){ return; }

        // InputFile2
        try{
            String file = inputFile2+"\\coordinator_stats.txt";
            FileReader fileReader = new FileReader(file);
            fileReader.close();
        }
        catch(java.io.FileNotFoundException e){ return; }

        // Get the errors as string line(for one file)
        getErrorGTFile(inputFile1,outputFile);

        BufferedReader br1 = new BufferedReader(new FileReader(inputFile1+"\\coordinator_stats.txt")); // input file1 as args[0]
        BufferedReader br2 = new BufferedReader(new FileReader(inputFile2+"\\coordinator_stats.txt")); // input file2 as args[1]
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        String line1 = br1.readLine();
        String line2 = br2.readLine();
        int querySize = 0;
        double error = 0d;
        double avg_abs_error = 0d;
        int lbv = 0,ubv = 0;

        String errors = "";
        String errors_runs = "";
        String errors_runs_200 = "";
        double tmp_error = 0;

        while ( line1 != null && line2 != null ) {

            line1 = line1.replaceAll("\0","");
            line2 = line2.replaceAll("\0","");

            if( !line1.contains("Input : Input =>") ){ line1 = br1.readLine(); }
            if( !line2.contains("Input : Input =>") ){ line2 = br2.readLine(); }

            if(  line1 != null && line2 != null && line1.contains("Input : Input =>") && line2.contains("Input : Input =>") ){

                // Process the inputs from the two files
                // Split the lines based on =>
                String[] splitsLine1 = line1.split("=>")[1].trim().split(" , ");
                String value1 = null; String estProb1 = null; String truthProb1 = null;

                String[] splitsLine2 = line2.split("=>")[1].trim().split(" , ");
                String value2 = null; String estProb2 = null; String truthProb2 = null;

                // For the first line
                for(int i=0;i<splitsLine1.length;i++){
                    if(splitsLine1[i].contains("value")) value1 = splitsLine1[i].split(":")[1].trim();
                    if(splitsLine1[i].contains("estimated probability")) estProb1 =  splitsLine1[i].split(":")[1].trim();
                    if(splitsLine1[i].contains("truth probability")) truthProb1 =  splitsLine1[i].split(":")[1].trim();
                }

                // For the second line
                for(int i=0;i<splitsLine2.length;i++){
                    if(splitsLine2[i].contains("value")) value2 = splitsLine2[i].split(":")[1].trim();
                    if(splitsLine2[i].contains("estimated probability")) estProb2 = splitsLine2[i].split(":")[1].trim();
                    if(splitsLine1[i].contains("truth probability")) truthProb2 =  splitsLine2[i].split(":")[1].trim();
                }

                // Check the value
                value1 = value1.trim(); value2 = value2.trim();
                if(!value1.equals(value2)){
                    System.out.println("Count : "+querySize+" , queries are not equal");
                    return;
                }

                // Check ground truth probabilities
                if(Double.parseDouble(truthProb1.split(",")[0]) - Double.parseDouble(truthProb2.split(",")[0]) != 0){
                    System.out.println("Count : "+querySize+" , queries are not equal");
                    return;
                }

                // Find the error between two lines(estimated-exact)
                double estProb = Double.parseDouble(estProb1.split(",")[0]);
                double exactProb = Double.parseDouble(estProb2.split(",")[0]);
                double errorLine = estProb - exactProb;
                System.out.println((querySize+1)+" )" + -eps+" <= ln(estimated) - ln(truth) = " + (estProb-exactProb) + " <= "+eps );
                // writer.write((querySize+1)+" )" + " -0.1 <= ln(estimated) - ln(truth) = " + (estProb-exactProb) + " <= 0.1\n");

                // Check the bounds
                // Up to two decimal places precession : Precision.equals(0.104d,0.1d,0.01);
                if( (estProb-exactProb) >= eps ){ ubv++; }
                else if( (estProb-exactProb) <= -eps ){ lbv++; }

                // Append the error
                errors = errors.concat(errorLine +",");

                // Update the error
                error += errorLine;
                avg_abs_error += Math.abs(errorLine);

                // Update
                querySize++;

                // Append the avg error per 200 queries and calculate the avg error per 200 queries
                tmp_error += errorLine;
                if( querySize != 0 && querySize % 200 == 0 ){
                    tmp_error = tmp_error/200;
                    errors_runs = errors_runs.concat(tmp_error+",");
                    errors_runs_200 = errors_runs_200.concat((error / querySize) +",");
                    tmp_error = 0;
                }

                // Read the next lines
                line1 = br1.readLine();
                line2 = br2.readLine();
            }

        }

        System.out.println("Error to EXACTMLE");
        writer.write("\n\nError to EXACTMLE\n");

        System.out.println("Queries size : " + querySize);
        writer.write("Queries size : " + querySize + "\n");

        System.out.println( "Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv);
        writer.write("Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv + "\n");

        System.out.println( "\nAverage absolute error : " + avg_abs_error/querySize);
        writer.write( "Average absolute error : " + avg_abs_error/querySize + "\n");

        System.out.println( "Errors to EXACTMLE : " + errors.substring(0,errors.length()-1));
        writer.write( "Errors to EXACTMLE : " + errors.substring(0,errors.length()-1) + "\n");

        System.out.println( "Errors runs : " + errors_runs.substring(0,errors_runs.length()-1));
        writer.write( "Errors runs : " + errors_runs.substring(0,errors_runs.length()-1) + "\n");

        System.out.println( "Errors runs 200 : " + errors_runs_200.substring(0,errors_runs_200.length()-1));
        writer.write( "Errors runs 200 : " + errors_runs_200.substring(0,errors_runs_200.length()-1) + "\n");


        // Close buffers
        br1.close();
        br2.close();
        writer.close();

    }

    // Get the runtime and write the results to file
    public static void getRuntimeFile(String inputFile,String outputFile) throws IOException {

        try{
            String file = inputFile+"\\coordinator_stats.txt";
            FileReader fileReader = new FileReader(file);
            fileReader.close();
        }
        catch(java.io.FileNotFoundException e){ return; }

        BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\coordinator_stats.txt"));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        String line;
        String firstTs = "";
        String lastTs = "";

        line = br1.readLine();
        while ( line != null ) {

            line = line.replaceAll("\0", "");
            if (!line.contains("received")) { line = br1.readLine(); }

            // Get the first and last timestamp
            if (line != null && line.contains("received first")) {

                firstTs = line;

                // Read the next line
                line = br1.readLine();
            }
            if (line != null && line.contains("received last")) {

                lastTs = line;

                // Read the next line
                line = br1.readLine();
            }


        }

        // Get the runtime
        String[] splitsFirstLine = firstTs.split(" , ");
        String[] splitsLastLine = lastTs.split(" , ");

        long first = Long.parseLong(splitsFirstLine[1].trim());
        long last = Long.parseLong(splitsLastLine[1].trim());
        System.out.println("Runtime : "+(last-first)/1000d+" sec");
        writer.write("\n\nRuntime : "+(last-first)/1000d+" sec"+"\n");

        // Close buffers
        br1.close();
        writer.close();

    }

    // Calculate the throughput and write the results to file
    public static void getThroughputFile(String inputFile,String outputFile,int workers) throws IOException{

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        List<Double> finalThroughput = new ArrayList<>();
        List<Double> finalThroughputInstant = new ArrayList<>();
        List<Double> finalThroughputFirstLast = new ArrayList<>();

        // For all workers
        for(int j=0;j<workers;j++) {

            // Input
            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));

            String line;
            List<String> events = new ArrayList<>();
            List<Double> throughputs = new ArrayList<>();
            List<Double> throughputsInstant = new ArrayList<>();
            List<Double> processingTime = new ArrayList<>();

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("event")) { line = br1.readLine(); }

                // Get and add the event
                if (line != null && line.contains("event")) {

                    // Add the event
                    events.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            double throughput = 0d;
            long firstTimestamp = 0L;
            long firstCount = 0L;
            long secondTimestamp = 0L;
            long secondCount = 0L;
            events.remove(0);

            // Calculate the throughput using diff
            for (int i = 0; i <= events.size(); i++) {

                // Calculate the throughput
                if (i < (events.size() - 1)) {

                    // First row
                    String[] splitsFirstLine = events.get(i).split(" , ");
                    firstTimestamp = Long.parseLong(splitsFirstLine[1].trim());
                    firstCount = Long.parseLong(splitsFirstLine[2].split(" ")[2].trim());

                    // Second row
                    String[] splitsSecondLine = events.get(i + 1).split(" , ");
                    secondTimestamp = Long.parseLong(splitsSecondLine[1].trim());
                    secondCount = Long.parseLong(splitsSecondLine[2].split(" ")[2].trim());

                    // Throughput
                    if(secondTimestamp == firstTimestamp) continue;
                    throughput = (secondCount - firstCount) / ((secondTimestamp - firstTimestamp) / 1000d);
                    throughputs.add(throughput);
                }
            }

            // Calculate the instant throughput
            firstTimestamp = Long.parseLong(events.get(0).split(" , ")[1].trim());
            throughputsInstant.add(0d);
            processingTime.add(0d);
            for (int i = 1; i < events.size(); i++) {

                // Event
                String[] splitsFirstLine = events.get(i).split(" , ");
                secondTimestamp = Long.parseLong(splitsFirstLine[1].trim());
                secondCount = Long.parseLong(splitsFirstLine[2].split(" ")[2].trim());

                if(secondTimestamp == firstTimestamp) continue;
                double proc = ((secondTimestamp - firstTimestamp) / 1000d);
                processingTime.add(proc);
                throughputsInstant.add(secondCount/proc);
            }

            // First timestamp
            String[] splitsFirstLine = events.get(0).split(" , ");
            long timestamp = Long.parseLong(splitsFirstLine[1].trim());
            long count = Long.parseLong(splitsFirstLine[2].split(" ")[2].trim());

            // Last timestamp
            String[] splitsLastLine = events.get(events.size() - 1).split(" , ");
            long lastTimestamp = Long.parseLong(splitsLastLine[1].trim());
            long lastCount = Long.parseLong(splitsLastLine[2].split(" ")[2].trim());

            // Last
            throughput = (lastCount - count) / ((lastTimestamp - timestamp) / 1000d);
            double lastThroughput = throughput;
            // throughputs.add(throughput);


            // Statistics

            // Instant
            System.out.println("Worker "+j+" => Throughputs Instant : " + throughputsInstant);
            writer.write("\n\nWorker "+j+" => Throughputs Instant : " + throughputsInstant+"\n");

            System.out.println("Worker "+j+" => Processing Time : " + processingTime);
            writer.write("Worker "+j+" => Processing Time : " + processingTime+"\n");

            System.out.println("Worker "+j+" => Count Throughput Instant : " + throughputsInstant.size());
            writer.write("Worker "+j+" => Count Throughput Instant : " + throughputsInstant.size()+"\n");

            // Get the average throughput
            throughput = throughputsInstant.stream().mapToDouble(a -> a).average().getAsDouble();
            System.out.println("Worker "+j+" => Average Throughput Instant : " + throughput+"\n");
            writer.write("Worker "+j+" => Average Throughput Instant : " + throughput+"\n");

            // Update the final instant throughput of system
            finalThroughputInstant.add(throughput);


            // Diff
            // System.out.println("Worker "+j+" => Throughputs : " + throughputs);
            System.out.println("w"+j+" = " + throughputs);
            writer.write("\nw"+j+" = " + throughputs+"\n");

            System.out.println("Worker "+j+" => Count : " + throughputs.size());
            writer.write("Worker "+j+" => Count : " + throughputs.size()+"\n");

            System.out.println("Worker "+j+" => Min Throughput : " + throughputs.stream().min(Double::compareTo).get());
            writer.write("Worker "+j+" => Min Throughput : " + throughputs.stream().min(Double::compareTo).get()+"\n");

            System.out.println("Worker "+j+" => Max Throughput : " + throughputs.stream().max(Double::compareTo).get());
            writer.write("Worker "+j+" => Max Throughput : " + throughputs.stream().max(Double::compareTo).get()+"\n");

            // Last
            System.out.println("Worker "+j+" => Last(first-end) Throughput : "+lastThroughput);
            writer.write("Worker "+j+" => Last(first-end) Throughput : "+lastThroughput+"\n");

            // Update the final throughput of system
            finalThroughputFirstLast.add(lastThroughput);

            // Diff
            // Get the average throughput
            throughput = throughputs.stream().mapToDouble(a -> a).average().getAsDouble();
            System.out.println("Worker "+j+" => Average Throughput : " + throughput+"\n");
            writer.write("Worker "+j+" => Average Throughput : " + throughput+"\n");

            // Update the final throughput of system
            finalThroughput.add(throughput);

            // Close the buffer
            br1.close();
        }

        // Diff
        System.out.println("Final Throughputs : "+finalThroughput);
        writer.write("\n\nFinal Throughputs : "+finalThroughput+"\n");

        System.out.println("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get());
        writer.write("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get()+"\n");

        System.out.println("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get());
        writer.write("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get()+"\n");

        System.out.println("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum()+"\n");

        // Last
        System.out.println("Final Throughputs(First-Last) : "+finalThroughputFirstLast);
        writer.write("\n\nFinal Throughputs(First-Last) : "+finalThroughputFirstLast+"\n");

        System.out.println("Final throughput(First-Last) : "+finalThroughputFirstLast.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput(First-Last) : "+finalThroughputFirstLast.stream().mapToDouble(a -> a).sum()+"\n");

        // Instant
        System.out.println("Final Throughputs Instant : "+finalThroughputInstant);
        writer.write("\n\nFinal Throughputs Instant : "+finalThroughputInstant+"\n");

        System.out.println("Final throughput Instant : "+finalThroughputInstant.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput Instant : "+finalThroughputInstant.stream().mapToDouble(a -> a).sum()+"\n");

        // Close buffer
        writer.close();
    }

    // Get the tuples and processing time and write the results to file
    public static void getTuplesProcFile(String inputFile,String outputFile,int workers) throws IOException{

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        List<Double> finalThroughput = new ArrayList<>();
        List<Double> finalThroughputDiff = new ArrayList<>();
        List<Double> avgThroughput = new ArrayList<>();

        // For all workers
        for(int j=0;j<workers;j++) {

            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));

            String line;
            List<String> timers = new ArrayList<>();
            List<Long> numTuples = new ArrayList<>();
            List<Double> processingTime = new ArrayList<>();
            List<Double> throughputs = new ArrayList<>();
            String firstEvent = null;

            List<Double> throughputsDiff = new ArrayList<>();
            List<Double> processingTimeDiff = new ArrayList<>();

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("timer")) { line = br1.readLine(); }
                if(line != null && line.contains("receive first event")) firstEvent = line;

                // Get and add the timer
                if (line != null && line.contains("timer")) {

                    // Add the timer
                    timers.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            double throughput = 0d;
            long firstTimestamp = 0L;
            timers.remove(0);

            // Initialize
            numTuples.add(0L);
            processingTime.add(0d);
            throughputs.add(0d);
            firstTimestamp = Long.parseLong(firstEvent.split(" , ")[1].trim());

            // Calculate throughput
            for (int i=0;i<timers.size();i++) {

                // Split the line
                String[] splits = timers.get(i).split(" , ");

                // Get the processing time
                processingTime.add((Long.parseLong(splits[1].trim()) - firstTimestamp) / 1000d);

                // Get the number of tuples
                numTuples.add(Long.parseLong(splits[2].split(" ")[2].trim()));

                // Find the throughput
                if(processingTime.get(i+1) >= 1d){ throughputs.add(numTuples.get(i+1)/processingTime.get(i+1)); }

            }

            // Calculate throughputDiff
            throughputsDiff.add(0d);
            processingTimeDiff.add(0d);
            for (int i=0;i<processingTime.size()-1;i++) {

                if(processingTime.get(i) == 0d) continue;

                // Find the difference between processing time
                double diffProc = processingTime.get(i) - processingTime.get(i-1);

                // Find the difference between number of tuples
                long diffTuples = numTuples.get(i) - numTuples.get(i-1);

                // Calculate the throughput
                if(diffProc == 0d) continue;
                throughputsDiff.add(diffTuples/diffProc);
                processingTimeDiff.add(diffProc);

            }

            // Statistics
            // System.out.println("w"+j+" = " + throughputs);
            System.out.println("Worker "+j+" => NumTuples : " + numTuples);
            writer.write("\n\nWorker "+j+" => NumTuples : " + numTuples+"\n");

            System.out.println("Worker "+j+" => Processing Time : " + processingTime);
            writer.write("Worker "+j+" => Processing Time : " + processingTime+"\n");

            System.out.println("Worker "+j+" => Throughput : " + throughputs);
            writer.write("Worker "+j+" => Throughput : " + throughputs+"\n");

            System.out.println("Worker "+j+" => Processing Time Diff : " + processingTimeDiff);
            writer.write("Worker "+j+" => Processing Time Diff : " + processingTimeDiff+"\n");

            System.out.println("Worker "+j+" => ThroughputDiff : " + throughputsDiff);
            writer.write("Worker "+j+" => ThroughputDiff : " + throughputsDiff+"\n");

            System.out.println("Worker "+j+" => Count proc diff : " + processingTimeDiff.size());
            writer.write("Worker "+j+" => Count proc diff : " + processingTimeDiff.size()+"\n");

            System.out.println("Worker "+j+" => Count throughtputs diff : " + throughputsDiff.size());
            writer.write("Worker "+j+" => Count throughtputs diff: " + throughputsDiff.size()+"\n");

            System.out.println("Worker "+j+" => Count proc : " + processingTime.size());
            writer.write("Worker "+j+" => Count proc : " + processingTime.size()+"\n");

            System.out.println("Worker "+j+" => Count throughtputs : " + throughputs.size());
            writer.write("Worker "+j+" => Count throughtputs : " + throughputs.size()+"\n");

            System.out.println("Worker "+j+" => Count num : " + numTuples.size());
            writer.write("Worker "+j+" => Count num : " + numTuples.size()+"\n");

            System.out.println("Worker "+j+" => Min number of tuples : " + numTuples.stream().min(Long::compareTo).get());
            writer.write("Worker "+j+" => Min number of tuples : " + numTuples.stream().min(Long::compareTo).get()+"\n");

            System.out.println("Worker "+j+" => Max number of tuples : " + numTuples.stream().max(Long::compareTo).get());
            writer.write("Worker "+j+" => Max number of tuples : " + numTuples.stream().max(Long::compareTo).get()+"\n");

            // Calculate the average throughput using last
            throughput = numTuples.get(numTuples.size()-1)/processingTime.get(processingTime.size()-1);
            avgThroughput.add(throughput);
            System.out.println("Worker "+j+" => Average(Last) Throughput : " + throughput);
            writer.write("Worker "+j+" => Average(Last) Throughput : " + throughput+"\n");

            // Calculate the average throughput
            throughput = throughputs.stream().mapToDouble(a -> a).sum()/throughputs.size();
            System.out.println("Worker "+j+" => Average Throughput : " +throughput);
            writer.write("Worker "+j+" => Average Throughput : " +throughput+"\n");

            // Update the final throughput of system
            finalThroughput.add(throughput);

            // Calculate the average throughput using diff
            throughput = throughputsDiff.stream().mapToDouble(a -> a).sum()/throughputsDiff.size();
            System.out.println("Worker "+j+" => Average Throughput Diff : " +throughput+"\n");
            writer.write("Worker "+j+" => Average Throughput Diff : " +throughput+"\n");

            // Update the final throughput of system using diff
            finalThroughputDiff.add(throughput);


            // Close the buffer
            br1.close();
        }

        System.out.println("Final Throughputs : "+finalThroughput);
        writer.write("\nFinal Throughputs : "+finalThroughput+"\n");

        System.out.println("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get());
        writer.write("Min throughput : "+finalThroughput.stream().min(Double::compareTo).get()+"\n");

        System.out.println("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get());
        writer.write("Max throughput : "+finalThroughput.stream().max(Double::compareTo).get()+"\n");

        System.out.println("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput : "+finalThroughput.stream().mapToDouble(a -> a).sum()+"\n");

        System.out.println("Avg/Last throughput : "+avgThroughput.stream().mapToDouble(a -> a).sum());
        writer.write("\nAvg/Last throughput : "+avgThroughput.stream().mapToDouble(a -> a).sum()+"\n");

        System.out.println("Final Throughputs Diff : "+finalThroughputDiff);
        writer.write("\nFinal Throughputs Diff : "+finalThroughputDiff+"\n");

        System.out.println("Final throughput Diff : "+finalThroughputDiff.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput Diff : "+finalThroughputDiff.stream().mapToDouble(a -> a).sum()+"\n");


        // Close buffer
        writer.close();
    }

    // Calculate the tuples vs processing time for each worker and write the results to file
    public static void getTuplesFile(String inputFile,String outputFile,int workers) throws IOException{

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile+"\\communication_cost",true));

        // Total tuples
        long totalTuples = 0L;

        // Final throughput
        List<Double> finalThroughput = new ArrayList<>();

        // For all workers
        for(int j=0;j<workers;j++) {

            // Input
            BufferedReader br1 = new BufferedReader(new FileReader(inputFile+"\\worker_stats_" + j + ".txt"));

            String line;
            List<String> events = new ArrayList<>();
            List<Long> tuples = new ArrayList<>();
            List<Double> time = new ArrayList<>();

            line = br1.readLine();
            while (line != null) {

                line = line.replaceAll("\0", "");
                if (!line.contains("event")) { line = br1.readLine(); }

                // Get and add the event
                if (line != null && line.contains("event")) {

                    // Add the event
                    events.add(line);

                    // Read the next line
                    line = br1.readLine();
                }
            }

            // Process the events
            long firstTimestamp;
            events.remove(0);

            // Get the first timestamp
            String[] splitsFirstLine = events.get(0).split(" , ");
            firstTimestamp = Long.parseLong(splitsFirstLine[1].trim());
            time.add(0d);tuples.add(0L);

            for (int i = 1; i < events.size(); i++) {

                String[] splits = events.get(i).split(" , ");

                // Get total count,add to list
                tuples.add(Long.parseLong(splits[2].split(" ")[2].trim()));

                // Get the processing time,add to list
                time.add(((Long.parseLong(splits[1].trim()) - firstTimestamp)/1000d));
            }

            // Calculate throughput
            List<Double> throughput = new ArrayList<>();
            for(int i=0;i<tuples.size();i++){
                if(tuples.get(i) == 0) throughput.add(0d);
                else throughput.add(tuples.get(i)/time.get(i));

            }

            // Statistics
            System.out.println("Worker "+j+" => Tuples : " + tuples);
            writer.write("\n\nWorker "+j+" => Tuples : " + tuples+"\n");

            System.out.println("Worker "+j+" => Processing time : " + time);
            writer.write("Worker "+j+" => Processing time : " + time+"\n");

            System.out.println("Worker "+j+" => Throughput : " + throughput);
            writer.write("Worker "+j+" => Throughput : " + throughput+"\n");

            System.out.println("Size : "+tuples.size()+"(tuples)"+","+time.size());
            writer.write("Size : "+tuples.size()+"(tuples)"+","+time.size()+"\n");

            // Get the average throughput and update the final throughput
            double avgThroughput = throughput.stream().mapToDouble(a -> a).average().getAsDouble();
            finalThroughput.add(avgThroughput);

            // Update total tuples
            totalTuples += tuples.get(tuples.size()-1);

            br1.close();
        }

        System.out.println("Final Throughputs(Third way) : "+finalThroughput);
        writer.write("\n\nFinal Throughputs(Third way) : "+finalThroughput+"\n");

        System.out.println("Final throughput(Third way) : "+finalThroughput.stream().mapToDouble(a -> a).sum());
        writer.write("Final throughput(Third way) : "+finalThroughput.stream().mapToDouble(a -> a).sum()+"\n");

        System.out.println("Total tuples processed : " + totalTuples);
        writer.write("\nTotal tuples processed : " + totalTuples+"\n");



        // Close buffer
        writer.close();
    }

}
