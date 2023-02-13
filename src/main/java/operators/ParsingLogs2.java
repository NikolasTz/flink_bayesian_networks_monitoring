package operators;

import java.io.*;
import java.util.ArrayList;
import java.util.Objects;


public class ParsingLogs2 {

    public static void main(String[] args) throws Exception {


        // This class used for error relative to MLE experiment
        // compareFiles("/home/nsimos/IdeaProjects/flink_bayesian_networks_monitoring(1)/queries1","/home/nsimos/IdeaProjects/flink_bayesian_networks_monitoring(1)/queries2");
        // collectMessages("alarm","NON_UNIFORM");
        // collectMessages("hepar2","NON_UNIFORM");
        // collectMessages("hepar2","RC");
        // collectMessages("alarm","RC");

        // First input-Approximate counter
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries1")); // input file as args[0]
        BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries1")); // input file as args[0]
        // BufferedReader br1 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\alarm\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\queries1"));

        // Second input-Exact counter
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries2")); // input file as args[0]
        BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\IdeaProjects\\flink_bayesian_networks_monitoring\\queries1-U")); // input file as args[0]
        // BufferedReader br2 = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\alarm\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\queries2")); // input file as args[0]


        // Buffering all the queries from queries2 file
        String line2 = br2.readLine();
        ArrayList<String> queries2 = new ArrayList<>();
        while ( line2 != null ) {

            line2 = line2.replaceAll("\0","");

            if( line2.contains("Input") ){ queries2.add(line2);}

            // Read the next lines
            line2 = br2.readLine();
        }

        // Close buffer
        br2.close();
        System.out.println("Size of queries2 is : "+queries2.size());

        // Output
        BufferedWriter writer = new BufferedWriter(new FileWriter("errorMLE"));
        String line1 = br1.readLine();
        int querySize = 0;
        double error = 0d;
        int lbv = 0,ubv = 0;
        boolean find = false;

        while ( line1 != null ) {

            line1 = line1.replaceAll("\0","");

            if( !line1.contains("Input") ){ line1 = br1.readLine(); }

            if( line1 != null && line1.contains("Input") ){

                // Process the inputs from the two files

                // Split the lines based on colon
                String[] splitsLine1 = line1.split(":");
                String value1 = null; String estProb1 = null;

                String[] splitsLine2;
                String value2 = null; String estProb2 = null;

                // For the first line
                for(int i=0;i<splitsLine1.length;i++){
                    if(splitsLine1[i].contains("value")) value1 = splitsLine1[i+1];
                    if(splitsLine1[i].contains("estimated probability")) estProb1 =  splitsLine1[i+1];
                }

                // For each query from queries2 file , check for line1
                for( String query : queries2 ){

                    line2 = query;
                    splitsLine2 = line2.split(":");
                    value2 = null; estProb2 = null;

                    // For the second line
                    for(int i=0;i<splitsLine2.length;i++){
                        if(splitsLine2[i].contains("value")) value2 = splitsLine2[i+1];
                        if(splitsLine2[i].contains("estimated probability")) estProb2 = splitsLine2[i+1];
                    }

                    // Check the value
                    value1 = value1.trim(); value2 = value2.trim();
                    if(value1.equals(value2)){
                        find = true;
                        break;
                    }
                }

                // Check the value
                // value1 = value1.trim(); value2 = value2.trim();
                if( !find ) System.out.println( "Count : "+querySize+" Query not found !!! ");
                if(!value1.equals(value2)){
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

                // Update the error
                error += errorLine;

                // Read the next lines
                line1 = br1.readLine();
                querySize++;
            }

            // Update the find flag
            find = false;

        }

        System.out.println("Queries size : " + querySize);
        writer.write("Queries size : " + querySize + "\n");

        System.out.println( "Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv);
        writer.write("Average error : " + error/querySize + " , lower violations : " + lbv + " , upper violations : " + ubv + "\n");


        // Close buffers
        br1.close();
        writer.close();

        // compareFiles("coordinator_stats","coordinator_stats.txt");
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

    // Read file and collect the number of messages
    // collectMessages("worker0_messagesTest");
    // collectMessages("coordinator_messages");
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

        // For coordinator file the numMessages = num_round of each Counter - RANDOMIZED case

        while ( line != null ){

            line = line.replaceAll("\0","");

            // Skip new and empty lines
            if(line.equals("\n") || line.equals("")){
                line = br.readLine();
                continue;
            }

            // Get numMessagesSent and overheadMessages
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
                }
                
                // Update the messages
                messages += Long.parseLong(Objects.requireNonNull(message));

                // Update the total count
                totalCount++;
            }

            // Read the next line
            line = br.readLine();

        }

        System.out.println( "Total counters : " + totalCount
                            + " , Total messages : " +messages
                            + " , Zero messages : " + messagesZero
                            + ", Zero counters : " +zeroCounters
                            + " , numMessagesSent : " + numMessagesSent
                            + " , overheadMessages : " + overheadMessages);

        return messages;
    }

    // Collect the messages from workers and coordinator
    public static void collectMessages(String bayesianNetwork,String type) throws IOException {

        long totalMessagesWorker = 0L;
        long totalMessagesCoordinator;

        // Collect number of messages for all workers
        for(int i=0;i<8;i++){
            System.out.println("File : " + "worker" + i + ".messages");
            totalMessagesWorker += collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\worker"+i+".messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\worker"+i+".messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\worker"+i+".messages");
            // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE\\5K\\Experient_1\\"+type+"\\worker"+i+".messages");
        }

        // Collect number of messages for coordinator
        System.out.println("File : coordinator_messages");
        totalMessagesCoordinator = collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
        // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\coordinator_messages");
        // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Extensions\\RC_DC\\Dummy_Father\\50M\\Experiment_1\\NON_UNIFORM\\"+type+"\\coordinator_messages");
        // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE-DC\\50M\\Experiment_1\\"+type+"\\coordinator_messages");
        // collectMessages("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\results\\"+bayesianNetwork+"\\Error_to_MLE\\5K\\Experient_1\\"+type+"\\coordinator_messages");

        System.out.println("Total messages workers : " + totalMessagesWorker);
        System.out.println("Total messages coordinator : " + totalMessagesCoordinator);
    }

}
