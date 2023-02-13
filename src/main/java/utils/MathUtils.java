package utils;

import java.util.Arrays;

public class MathUtils {

    // *************************************************************************
    //                              MATH UTILS
    // *************************************************************************

    /**
     * Calculate the next power of 2 that is smaller or equal than x
     * @param x the number
     * @return  the highest power of 2 that is smaller than x
     */
    public static int highestPowerOf2(double x){
        if( x == 0 ) return 1;
        return (int)(Math.log(x) / Math.log(2));
    }

    public static int findLargestPowerOf2LessThanTheGivenNumberUsingLogBase2(double input) {

        double temp = input;
        if (input % 2 == 0) { temp = input - 1; } // For less than input , only for even numbers is needed

        return (int) (Math.log(temp) / Math.log(2));
    }

    /** Calculate the value which will be halved the probability
     * @param epsilon The epsilon of counter
     * @param k Number of workers
     * @return The value which will be halved the probability
     */
    public static int findValueProbHalved(double epsilon,int k){
        if( epsilon == 0 ) return 0;
        return (int) ((4*Math.sqrt(k)) / epsilon)+1;
    }


    /**  Calculate the c*sqrt(k)/eps,used exclusively from RANDOMIZED counters
     * @param k Number of workers
     * @param eps The epsilon of counter
     * @param c The rescaling constant of counter
     * @return The c*sqrt(k)/eps
     */
    public static long calculate(int k,double eps,double c){

        if( eps == 0 ) return 0;
        return (long) ((c*Math.sqrt(k))/eps);
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

        return Math.log(numerator/denominator) / Math.log(2);
    }

    /** Calculate the value which will be halved the probability,used exclusively from RANDOMIZED counters
     * @param epsilon The epsilon of counter
     * @param k Number of workers
     * @param c Rescaling constant of counter
     * @return The value which will be halved the probability
     */
    public static long findValueProbHalved(double epsilon,int k,double c){
        if( epsilon == 0 ) return 0;
        return ((long) ((4*Math.sqrt(k)*c) / epsilon))+1;
    }

    /** Calculate the value which will be conditioned(halved,sub_tripled...) the probability,used exclusively from RANDOMIZED counters
     * @param epsilon The epsilon of counter
     * @param k Number of workers
     * @param c Rescaling constant of counter
     * @return The value which will be conditioned the probability
     */
    public static long findValueProbConditioned(double epsilon,int k,double c,long condition){
        if( epsilon == 0 || condition == 0) return 0;
        return ((long) ((Math.pow(2,condition)*Math.sqrt(k)*c) / epsilon))+1;
    }


    /** Calculate the eps*condition/numWorkers,used only for DETERMINISTIC counters
     * @param eps The epsilon of counter
     * @param condition The condition of counter
     * @param numWorkers Number of workers
     * @return The eps*condition/numWorkers
     */
    public static long calculateCondition(double eps,long condition,int numWorkers){

        if( numWorkers == 0) return 0;
        return (long) ((eps*condition)/numWorkers);
    }

    /**  Calculate the value which will be halved the condition ,used exclusively from DETERMINISTIC counters
     * @param eps The epsilon of counter
     * @param numWorkers Number of workers
     * @return The value which will be halved the condition
     */
    public static long findValueCondHalved(double eps,int numWorkers){

        if( numWorkers == 0) return 0;
        return (long) ((2L * numWorkers)/eps)+1;
    }

    /**  Calculate the value which will be conditioned the condition ,used exclusively from DETERMINISTIC counters
     * @param eps The epsilon of counter
     * @param numWorkers Number of workers
     * @return The value which will be conditioned the condition
     */
    public static long findValueConditioned(double eps,int numWorkers,long condition){

        if( numWorkers == 0 || condition == 0 ) return 0;
        return (long) ((condition * numWorkers)/eps)+1;
    }


    /** Solve the equation lnx = y
     * @param y The y
     * @return The x
     */
    public static double convertLn(double y){ return Math.exp(y); }

    /** Calculate the ln(x)
     * @param x The argument of ln.In case of the x = 0 then return 0
     * @return The ln(x)
     */
    public static double log(double x){
        if( x==0 ) return 0;
        return Math.log(x);
    }

    /** Calculate the log10(x)
     * @param x The argument of log10.In case of the x = 0 then return 0
     * @return The log10(x)
     */
    public static double log10(double x){
        if( x==0 ) return 0;
        return Math.log10(x);
    }

    /** Calculate the log2(x)
     * @param x The argument of log2.In case of the x = 0 then return 0
     * @return The log2(x)
     */
    public static double log2(double x){
        if( x==0 ) return 0;
        return Math.log(x)/Math.log(2);
    }


    // *************************************************************************
    //                              ARRAY and VECTOR UTILS
    // *************************************************************************

    // Maybe move to other package and class ???

    /** Scale a row or column vector with constant c
     * @param vector Row or column vector
     * @param c The scaling constant
     * @return The scaled vector
     */
    public static double[] scaleVector(double[] vector,double c){
        
        // Corner cases
        if( vector == null || vector.length == 0 ) return null;
        
        // The new vector
        double[] newVector = new double[vector.length];

        // Scaling
        for(int i=0;i<vector.length;i++){ newVector[i] = vector[i]*c; }

        return newVector;
    }

    /** Scale a 2D array with constant c
     * @param array The two-dimensional array
     * @param c The scaling constant
     * @return The scaled vector
     */
    public static double[][] scaleArray(double[][] array,double c){

        // Corner cases
        if( array == null || array.length == 0 ) return null;

        // The new array
        double[][] newArray = new double[array.length][array[0].length];

        // Scaling
        for(int i=0;i<array.length;i++){
            for(int j=0;j<array[0].length;j++){
                newArray[i][j] = array[i][j]*c;
            }
        }

        return newArray;
    }

    /** Calculate the norm of vector of order p
     * @param vector The vector
     * @param order The order of norm
     * @return The norm of vector
     */
    public static double norm(double[] vector,int order){

        // Corner cases
        if( vector == null || order < 0 ) return 0d;

        // Norm
        double norm = 0d;

        // Calculate the norm
        for (double value : vector) { norm += Math.pow(value, order); }

        norm = Math.pow(norm,1.0d/order);

        return norm;
    }

    /** Convert a 2D array to vector using the row major method
     * @param array The 2D array
     * @return The vector
     */
    public static double[] rowMajor(double[][] array){

        // Corner cases
        if( array == null || array.length == 0 ) return null;

        // The new vector
        double[] vector = new double[array.length*array[0].length];
        int index = 0;

        for (double[] row : array) {
            for (int j = 0; j < array[0].length; j++) {
                vector[index] = row[j];
                index++;
            }
        }

        return vector;
    }

    /**  Convert a 2D array to vector using the column major method
     * @param array The 2D array
     * @return The vector
     */
    public static double[] columnMajor(double[][] array){

        // Corner cases
        if( array == null || array.length == 0 ) return null;

        // The new vector
        double[] vector = new double[array.length*array[0].length];
        int index = 0;

        for(int i=0;i<array[0].length;i++){
            for(int j=0;j<array.length;j++){
                vector[index] = array[j][i];
                index++;
            }
        }

        return vector;
    }

    /** Convert a vector to 2D array
     * @param vector The vector
     * @param major Determine the way how the vector originated. True correspond to row and false to column major
     * @param depth The depth of 2D array
     * @param width The width of 2D array
     * @return The 2D array
     */
    public static double[][] convert2D(double[] vector,boolean major,int depth,int width){

        // Corner cases
        if( vector == null || vector.length == 0 || width <= 0 || depth <= 0 ) return null;

        // The new 2D array
        double[][] array = new double[depth][width];
        int index = 0;

        // Row major
        if(major){
            for(int i=0;i<depth;i++){
                for(int j=0;j<width;j++){
                    array[i][j] = vector[index];
                    index++;
                }
            }
        }
        // Column major
        else{
            for(int i=0;i<width;i++){
                for(int j=0;j<depth;j++){
                    array[j][i] = vector[index];
                    index++;
                }
            }
        }

        return array;
    }

    /** Add two vectors
     * @param vector1 The first vector
     * @param vector2 The second vector
     * @return The vector which resulted from the addition
     */
    public static double[] addVectors(double[] vector1,double[] vector2){

        // Corner cases
        if( vector1 == null || vector2 == null || vector1.length == 0 || vector2.length == 0 ) return null;
        if( vector1.length != vector2.length ) return null;

        // The new vector
        double[] newVector = new double[vector1.length];

        for(int i=0;i<vector1.length;i++){ newVector[i] = vector1[i] + vector2[i]; }

        return newVector;

    }

    /** Add two arrays
     * @param array1 The first array
     * @param array2 The second array
     * @return The array which resulted from the addition
     */
    public static double[][] addArrays(double[][] array1,double[][] array2){

        // Corner cases
        if( array1 == null || array2 == null || array1.length == 0 || array2.length == 0 ) return null;
        if( array1.length != array2.length || array1[0].length != array2[0].length ) return null;

        // The new array
        double[][] newArray = new double[array1.length][array1[0].length];

        for(int i=0;i<array1.length;i++){
            for(int j=0;j<array1[0].length;j++){
                newArray[i][j] = array1[i][j] + array2[i][j];
            }
        }

        return newArray;

    }

    /** Point-wise comparison of two vectors
     * @param vector1 The first one vector
     * @param vector2 The second one vector
     * @return True if vectors are equals otherwise false
     */
    public static boolean compareVector(double[] vector1,double[] vector2){

        // Corner cases
        if( vector1 == null | vector2 == null ) return false;
        if( vector1.length != vector2.length ) return false;

        for(int i=0;i<vector1.length;i++){
            if( vector1[i] != vector2[i] ) return false;
        }

        return true;
    }

    /** Point-wise comparison of two arrays
     * @param array1 The first one array
     * @param array2 The second one array
     * @return True if arrays are equals otherwise false
     */
    public static boolean compareArrays(double[][] array1,double[][] array2){

        // Corner cases
        if( array1 == null | array2 == null ) return false;
        if( array1.length != array2.length | array1[0].length != array2[0].length ) return false;

        for(int i=0;i<array1.length;i++){
            for(int j=0;j<array1[0].length;j++){
                if( array1[i][j] != array2[i][j] ) return false;
            }
        }

        return true;
    }

    /** Return the minimum value of array
     * @param array The 1D array
     * @return The minimum value of array
     */
    public static double minArray(double[] array){
        if( array == null ) return Double.MAX_VALUE;
        return Arrays.stream(array).min().orElse(Double.MAX_VALUE);
    }


    // *************************************************************************
    //                              LAPLACE SMOOTHING
    // *************************************************************************

    // Laplace smoothing only for zero parameters
    // Laplace-lambda smoothed , lambda = 1 then add-one smoothing where lambda : smoothing constant or pseudoCount
    // Laplace smoothing => Parameter = estimator_node+lambda / estimator_parent+lambda*values_of_parameter where lambda : smoothing constant or pseudoCount
    public static double laplaceSmoothing(double estimatorNode,double estimatorParent,int valuesAttr,double lambda){

        double prob;

        if( estimatorParent == 0 || estimatorNode == 0){ return 1.0d; } // Without Laplace Smoothing-ignore ???

        if( estimatorParent == 0 || estimatorNode == 0 ){
            if( estimatorParent != 0 ){ prob = lambda/(estimatorParent+(lambda*valuesAttr)); }
            else{ prob = lambda/(lambda*valuesAttr); }
            // System.out.println("Hello from laplace smoothing");
        }
        else{ prob = estimatorNode / estimatorParent; }

        return prob;

    }

}
