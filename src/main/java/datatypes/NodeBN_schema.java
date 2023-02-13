package datatypes;

import java.io.Serializable;
import java.util.*;

public class NodeBN_schema implements Serializable {

    private String name; // Name of node
    private int cardinality; // The cardinality of node domain
    private String[] values; // The values of node
    private NodeBN_schema[] parents; // The parents of node
    private String[] trueParameters; // The true parameters of node

    // Constructors

    // Empty constructor
    public NodeBN_schema(){}

    // Getters
    public String getName() { return name; }
    public int getCardinality() { return cardinality; }
    public String[] getValues() { return values; }
    public NodeBN_schema[] getParents() { return parents; }
    public String[] getTrueParameters() { return trueParameters; }

    // Setters
    public void setName(String name) { this.name = name; }
    public void setCardinality(int cardinality) { this.cardinality = cardinality; }
    public void setValues(String[] values) { this.values = values; }
    public void setParents(NodeBN_schema[] parents) { this.parents = parents; }
    public void setTrueParameters(String[] trueParameters) { this.trueParameters = trueParameters; }

    // Methods

    @Override
    public String toString(){
        return "Node => "
                + " name : " + this.name
                + " , cardinality : " + this.cardinality
                + " , values : " + Arrays.toString(this.values)
                + " , parents : " + Arrays.toString(this.parents)
                + " , trueParameters : " + Arrays.toString(this.trueParameters);
    }

    // Return the names of parents node
    public static ArrayList<String> getNameParents(NodeBN_schema node){

        // Name of parents nodes
        ArrayList<String> name_parents = new ArrayList<>();

        // Get the names of parents node
        for (NodeBN_schema parent : node.getParents()) { name_parents.add(parent.getName()); }

        return name_parents;

    }

    // Return the values of parents node
    public static List<Collection<String>> getValueParents(NodeBN_schema node){

        // The values of parents
        List<Collection<String>> values = new ArrayList<>();

        // Get the values of parents nodes
        for (NodeBN_schema parent : node.getParents()) {
            ArrayList<String> array = new ArrayList<>(Arrays.asList(parent.getValues()));
            values.add(array);
        }

        return values;
    }

    // Return the cardinality of parents domain(Ki)
    public int getCardinalityParents(){

        int cardinality = 0;

        if( this.getParents().length != 0 ) {
            cardinality = 1;
            for (NodeBN_schema parent : this.getParents()) {
                cardinality = cardinality * parent.getCardinality();
            }
        }

        return cardinality;
    }

    // Find the cardinality of parents nodes and node
    public static ArrayList<Integer> findCardinalities(NodeBN_schema[] nodes,ArrayList<String> parentsNames,ArrayList<String> nodeNames){

        ArrayList<Integer> cardinalities = new ArrayList<>();
        int cardinality_parents = 1;
        int cardinality_node = 1;

        int num_updates_parent = 0;
        int num_updates_node = 0;

        if( nodeNames == null ){ cardinality_node = 0; }
        if( parentsNames == null ){ cardinality_parents = 0; }

        for (NodeBN_schema node : nodes){

            if( nodeNames == null && parentsNames == null ) break;

            if( nodeNames != null && parentsNames != null ) {
                if( num_updates_node == nodeNames.size() && num_updates_parent == parentsNames.size()) break;
            }
            else if(nodeNames != null){
                if( num_updates_node == nodeNames.size()) break;
            }
            else{
                if( num_updates_parent == parentsNames.size()) break;
            }

            // Find the cardinality of parentsNames
            if( parentsNames != null && num_updates_parent < parentsNames.size() ) {
                for (String parentName : parentsNames) {
                    if ( node.getName().equals(parentName) ) {
                        cardinality_parents = cardinality_parents * node.getCardinality();
                        num_updates_parent++;
                    }
                }
            }

            // Find the cardinality of nodeNames
            if( nodeNames != null && num_updates_node < nodeNames.size() ) {
                for (String nodeName : nodeNames) {
                    if ( node.getName().equals(nodeName) ) {
                        cardinality_node = cardinality_node * node.getCardinality();
                        num_updates_node++;
                    }
                }
            }
        }

        cardinalities.add(cardinality_node);
        cardinalities.add(cardinality_parents);

        return cardinalities;
    }

    // Find the index of node based on name of node
    public static int findIndexNode(String nameNode,NodeBN_schema[] nodes){

        for(int i=0;i<nodes.length;i++){ if( nodes[i].getName().equals(nameNode)) return i; }

        // Not found
        return -1;
    }

    // Convention : The last node of NodeBN_schema array correspond to the class node and class counter/parameter => (nodeParents and nodeParentsValues) = null
    // Find the cardinality of class node of Naive Bayes Classifier(NBC)
    public static int findCardClass(NodeBN_schema[] nodes){

        // The class node always locate at last index of array
        return nodes[nodes.length-1].getCardinality();
    }

    // Find the cardinality of node based on name
    public static int findCardNode(NodeBN_schema[] nodes,String name){
        for(NodeBN_schema node : nodes){ if( node.getName().equals(name) ) return node.getCardinality(); }
        return 0; // Not found
    }

    // Find the maximum cardinality from all nodes
    public static int findMaxCardinality(NodeBN_schema[] nodes){

        int maxCardinality = 0;

        for(NodeBN_schema node : nodes){
            if( node.getCardinality() >= maxCardinality ){ maxCardinality = node.getCardinality(); }
        }

        return maxCardinality;
    }

    // Find the maximum number of parent nodes from all nodes
    public static int findMaxParents(NodeBN_schema[] nodes){

        int maxParents = 0;

        for (NodeBN_schema node : nodes){
            // Check if exists parents
            if( node.getParents().length > 0 ){
                if( node.getParents().length >= maxParents ){ maxParents = node.getParents().length; }
            }
        }

        return maxParents;
    }

}
