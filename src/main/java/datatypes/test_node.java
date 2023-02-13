package datatypes;

import com.fasterxml.jackson.databind.ObjectMapper;
import datatypes.counter.Counter;

import java.io.File;
import java.util.*;


public class test_node {

    public static void main(String[] args) throws Exception {

        // Bayesian Network(Earthquake)
        String jsonBN = "[{\"name\":\"Alarm\",\"trueParameters\":[\"0.95,0.94,0.29,0.001\",\"0.05,0.06,0.71,0.999\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"trueParameters\":[\"0.01\",\"0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"trueParameters\":[\"0.02\",\"0.98\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"trueParameters\":[\"0.9,0.05\",\"0.1,0.95\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"trueParameters\":[\"0.7,0.01\",\"0.3,0.99\"],\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";
        // String jsonBN = "[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]},{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"Burglary\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"Earthquake\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[ ] },{\"name\":\"JohnCalls\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] },{\"name\":\"MaryCalls\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[{\"name\":\"Alarm\",\"cardinality\":2,\"values\":[\"True\",\"False\"],\"parents\":[]}] }]";


        ObjectMapper objectMapper = new ObjectMapper();

        // Ignore Unknown JSON Fields on deserialization !!!
        // objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Unmarshall as collection
        //List<NodeBN_schema> list = objectMapper.readValue(jsonBN, List.class);

        // Unmarshall as array
        NodeBN_schema[] nodes = objectMapper.readValue(jsonBN, NodeBN_schema[].class); // read value from string
        //NodeBN_schema[] nodes = objectMapper.readValue(new File("src\\main\\java\\bayesianNetworks\\bn_JSON_alarm.json"), NodeBN_schema[].class); // read value from file or url

        // All the counters of BN
        Vector<Counter> counters = new Vector<>();

        for (NodeBN_schema node : nodes) {

            System.out.println(node.toString());
            counters.addAll(initializeCounters(node));

        }

        // Add the total count
        Counter total_count = new Counter();
        ArrayList<String> name = new ArrayList<>();
        name.add("total_count");
        total_count.setNodeName(name);
        total_count.setActualNode(false);

        // Add to counters
        counters.add(total_count);

        // Print all counters
        System.out.println("\nThe size of total counters is : "+counters.size());
        counters.forEach( counter -> System.out.println(counter.toString()));


    }

    // Go to workers , plus comments !!!
    public static ArrayList<Counter> initializeCounters(NodeBN_schema node){

        ArrayList<Counter> counters = new ArrayList<>();
        boolean first_time = true;

        for(int i=0;i<node.getValues().length;i++){

            // Get the parents
            if(node.getParents().length != 0 ){

                // Name of parents nodes
                ArrayList<String> name_parents = new ArrayList<>();

                // The values of parents
                List<Collection<String>> values = new ArrayList<>();

                // Get the names and values of parents nodes
                for (NodeBN_schema parent : node.getParents()) {
                    name_parents.add(parent.getName());
                    ArrayList<String> array = new ArrayList<>(Arrays.asList(parent.getValues()));
                    values.add(array);
                }

                // Generate the permutations
                Collection<List<String>> permutations = generatePermutations(values);

                // Generate the counters
                for (List<String> perm : permutations) {

                    // Get the permutation
                    LinkedList<String> permutation = (LinkedList<String>) perm;

                    // Generate the counter
                    Counter counter = generateCounter(node,i,true,name_parents,permutation);

                    // Add to counters
                    counters.add(counter);

                    if(first_time){
                        // Generate the parent node only one time per node
                        Counter parent_counter = generateCounter(null, -1, false, name_parents, permutation);

                        // Add to counters
                        counters.add(parent_counter);
                    }

                }

                first_time = false;
            }
            else{

                // Generate the counter
                Counter counter = generateCounter(node,i,true,null,null);

                // Add to counters
                counters.add(counter);

            }

        }

        return counters;
    }

    public static Counter generateCounter(NodeBN_schema node, int valueIndex, boolean actualNode, ArrayList<String> parentsName, LinkedList<String> parentsValues){

        // Counter
        Counter counter = new Counter();

        // Parent counter
        if(valueIndex == -1){
            counter.setNodeName(parentsName);
            counter.setNodeValue(new ArrayList<>(parentsValues));
            counter.setActualNode(actualNode);
            return counter;
        }

        // Set the name of node
        ArrayList<String> name = new ArrayList<>();
        name.add(node.getName());
        counter.setNodeName(name);

        // Set the value of node
        ArrayList<String> value = new ArrayList<>();
        value.add(node.getValues()[valueIndex]);
        counter.setNodeValue(value);

        // Marked as actual node
        counter.setActualNode(actualNode);

        // Set the name and values of parents node
        counter.setNodeParents(parentsName);
        if( parentsValues != null) counter.setNodeParentValues(new ArrayList<>(parentsValues));

        return counter;

    }

    /**
     * Combines several collections of elements and create permutations of all of them, taking one element from each
     * collection, and keeping the same order in resultant lists as the one in original list of collections.
     *
     * <ul>Example
     * <li>Input  = { {a,b,c} , {1,2,3,4} }</li>
     * <li>Output = { {a,1} , {a,2} , {a,3} , {a,4} , {b,1} , {b,2} , {b,3} , {b,4} , {c,1} , {c,2} , {c,3} , {c,4} }</li>
     * </ul>
     *
     * @param collections Original list of collections which elements have to be combined.
     * @return Resultant collection of lists with all permutations of original list.
     */
    public static <T> Collection<List<T>> generatePermutations(List<Collection<T>> collections) {
        if (collections == null || collections.isEmpty()) { return Collections.emptyList(); }
        else {
            Collection<List<T>> res = new LinkedList<>();
            permutations(collections, res, 0, new LinkedList<>());
            return res;
        }
    }

    /** Recursive implementation permutations
     *
     *
     * */
    private static <T> void permutations(List<Collection<T>> ori, Collection<List<T>> res, int d, List<T> current) {
        // If depth equals number of original collections, final reached, add and return
        if (d == ori.size()) {
            res.add(current);
            return;
        }

        // Iterate from current collection and copy 'current' element N times, one for each element
        Collection<T> currentCollection = ori.get(d);
        for (T element : currentCollection) {
            List<T> copy = new LinkedList<>(current);
            copy.add(element);
            permutations(ori, res, d + 1, copy);
        }
    }

}
