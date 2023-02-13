package fgm.sketch;


import fgm.datatypes.VectorType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static utils.Util.nextInt;
import static utils.Util.nextPrime;

public class CountMin extends VectorType implements Serializable {

    // External hash function used on project is xxh3 hash function for generating the key
    // Internal hash function used from this class is Murmur3 hash function
    // For functions estimate,update with argument String key will be used Murmur3 hash function to hashing the key

    private double[][] sketch;
    private int numOfCollisions = 0; // Number of collisions on updates

    // Carter and Wegman method , a polynomial hash function
    private final long prime = nextPrime(100); // 1125899906842597L;
    private final int alpha = nextInt((int) prime);
    private final int beta = nextInt((int) prime);

    // Coefficients for hash functions
    // One a,b for each hash function
    // Total hash function equal to width of sketch
    // Prime >= Length of keys
    private int[] alphas;
    private int[] betas;

    // Constructors
    public CountMin(){}
    public CountMin(int depth,int width){
        this.sketch = new double[depth][width];
        initializeCoefficients();
    }
    public CountMin(double epsilon,double delta){

        // Calculate width and depth
        int width = (int) Math.ceil(Math.exp(1)/epsilon);
        int depth = (int) Math.ceil(Math.log(1/delta));

        this.sketch = new double[depth][width];
        initializeCoefficients();
    }

    // Getters
    public double[][] getSketch() { return sketch; }
    public int getNumOfCollisions() { return numOfCollisions; }
    public int[] getAlphas() { return alphas; }
    public int[] getBetas() { return betas; }
    public long getPrime() { return prime; }
    public int getAlpha() { return alpha; }
    public int getBeta() { return beta; }

    // Setters
    public void setSketch(double[][] sketch) { this.sketch = sketch; }
    public void setNumOfCollisions(int numOfCollisions) { this.numOfCollisions = numOfCollisions; }
    public void setAlphas(int[] alphas) { this.alphas = alphas; }
    public void setBetas(int[] betas) { this.betas = betas; }


    // Methods

    public int depth(){ return this.sketch.length; } // The depth of sketch
    public int width(){ return this.sketch[0].length; } // The width of sketch
    public int size(){ return depth()*width(); } // The size of sketch that is number of entries
    public int sizeBytes(){ return depth()*width()*Long.BYTES; } // The size of sketch in bytes
    public double epsilon(){ return 1.0d / width(); } // return Math.exp(1)/ width();
    public double delta(){ return Math.exp(Math.log(1d)- depth()); }


    // Initialize coefficients for hash function
    // Hash function has the form of ax+b where a,b uniformly at random for every row
    // and define the prime p who is bigger than the length of keys
    public void initializeCoefficients(){

        // Initialize the arrays
        alphas = new int[depth()];
        betas = new int[depth()];

        // For every row choose uniformly at random the a,b
        for(int i = 0; i< depth(); i++){
            alphas[i] = nextInt((int) prime);
            betas[i] = nextInt((int) prime);
        }
    }

    // Update based hashcode
    public void updateHashCode(String key){

        // Get the hashCode
        long hashCode = key.hashCode();

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Estimate using hashcode
    public double estimateHashCode(String key){

        // Find the index
        long hashCode = key.hashCode();

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum of rows
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;
            if( hash < 0 ) hash = ~hash;

            // Get the index
            index = (int) (hash % width());

            min = Math.min(min,sketch[i-1][index]);
        }
        return min;
    }


    // Update based Array hashcode
    public void updateArrayHashCode(String key){

        // Get the hashCode
        long hashCode =  Arrays.hashCode(key.getBytes());

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Estimate using Array hashcode
    public double estimateArrayHashCode(String key){

        // Find the index
        long hashCode = Arrays.hashCode(key.getBytes());

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum of rows
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;
            if( hash < 0 ) hash = ~hash;

            // Get the index
            index = (int) (hash % width());

            min = Math.min(min,sketch[i-1][index]);
        }
        return min;
    }


    // Update based on hash function
    public void updateHash(String key){

        // Get the hashCode
        long hashCode = hash(key);

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo width
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Estimate using hash function
    public double estimateHash(String key){

        // Find the index
        long hashCode = hash(key);

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum of rows
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;
            if( hash < 0 ) hash = ~hash;

            // Get the index
            index = (int) ((hash) % width());

            min = Math.min(min,sketch[i-1][index]);
        }

        return min;
    }


    // Update based on murmur3 , a non-cryptographic hash function
    public void updateMurmur(String key){

        // Used the trick from "Less Hashing, Same Performance: Building a Better Bloom Filter"

        // Get the bytes of key
        byte[] bytes = key.getBytes();

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(bytes);
        long hash2 = (hash >>> 32);

        long hashing;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hash + (i*hash2);

            // Negative value => Flip all the bits
            if( hashing < 0 ) hashing = ~hashing;

            // Modulo with width of sketch
            index = (int) (hashing % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
        
    }

    // Update based on murmur3 , a non-cryptographic hash function
    public void updateMurmur(byte[] key){

        // Used the trick from "Less Hashing, Same Performance: Building a Better Bloom Filter"

        // Get the bytes of key

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(key);
        long hash2 = (hash >>> 32);

        long hashing;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hash + (i*hash2);

            // Negative value => Flip all the bits
            if( hashing < 0 ) hashing = ~hashing;

            // Modulo with width of sketch
            index = (int) (hashing % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Update based on murmur3 , a non-cryptographic hash function
    public void updateMurmur(long hashedKey){

        // Used the trick from "Less Hashing, Same Performance: Building a Better Bloom Filter"

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash2 = (hashedKey >>> 32);

        long hashing;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hashedKey + (i*hash2);

            // Negative value => Flip all the bits
            if( hashing < 0 ) hashing = ~hashing;

            // Modulo with width of sketch
            index = (int) (hashing % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Estimate using Murmur3
    public double estimateMurmur(String key){

        // Get the bytes of key
        byte[] bytes = key.getBytes();

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(bytes);
        long hash2 = (hash >>> 32);

        long hashing;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hash + (i*hash2);
            if( hashing < 0) hashing = ~hashing;

            // Find the index
            index = (int) (hashing % width());

            min = Math.min(min,sketch[i-1][index]);

        }

        return min;
    }

    // Estimate using Murmur3
    public double estimateMurmur(byte[] key){

        // Get the bytes of key

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(key);
        long hash2 = (hash >>> 32);

        long hashing;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hash + (i*hash2);
            if( hashing < 0) hashing = ~hashing;

            // Find the index
            index = (int) (hashing % width());

            min = Math.min(min,sketch[i-1][index]);

        }

        return min;
    }

    // Estimate using Murmur3
    public double estimateMurmur(long hashedKey){

        // Get the bytes of key

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash2 = (hashedKey >>> 32);

        long hashing;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hashing = hashedKey + (i*hash2);
            if( hashing < 0) hashing = ~hashing;

            // Find the index
            index = (int) (hashing % width());

            min = Math.min(min,sketch[i-1][index]);

        }

        return min;
    }

    // Update based on murmur3 , a non-cryptographic hash function , using polynomial hash function of the form ax+b
    public void updatePolyMurmur(String key){

        // Get the bytes of key
        byte[] bytes = key.getBytes();

        // Hashing the key
        long hash = Murmur3.hash64(bytes);

        long hashing;
        int index;

        // For each row update the hashing cell
        for (int i = 0; i< depth(); i++){

            // Hash function
            hashing = (alphas[i]*hash) + betas[i];

            // Convert negative hash
            if( hashing < 0) hashing = ~hashing;

            // Modulo with width of sketch
            index = (int) (hashing % width());

            // Update the number of collisions
            if( sketch[i][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i][index] += 1;
        }

    }

    // Estimate using Murmur3 , using polynomial hash function of the form ax+b
    public double estimatePolyMurmur(String key){

        // Get the bytes of key
        byte[] bytes = key.getBytes();

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(bytes);

        long hashing;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 0; i< depth(); i++){

            // Hash function
            hashing = (alphas[i]*hash) + betas[i];
            if( hashing < 0) hashing = ~hashing;

            // Find the index
            index = (int) (hashing % width());

            min = Math.min(min,sketch[i][index]);

        }

        return min;
    }


    // Update based on Carter and Wegman hash function
    public void updateCW(String key){

        // Get the hashcode of key
        long hashCode = Arrays.hashCode(key.getBytes()); // or key.hashCode();

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hash = ( alpha*(i*hashCode)+beta ) % prime;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;

        }
    }

    // Estimate using Carter and Wegman method
    public double estimateCW(String key){

        // Get the hashcode of key
        long hashCode = Arrays.hashCode(key.getBytes()); // or key.hashCode();

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 1; i<= depth(); i++){

            // Hashing
            hash = ( alpha*(i*hashCode)+beta ) % prime;
            if( hash < 0 ) hash = ~hash;

            // Find index
            index = (int) (hash % width());

            min = Math.min(min,sketch[i-1][index]);
        }

        return min;
    }


    // Update based on Carter and Wegman method , using different hash functions
    public void updateCWS(String key){

        // Get the hashcode of key
        long hashCode = key.hashCode();

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 0; i< depth(); i++){

            // Hashing
            hash = ( (alphas[i]*hashCode)+betas[i] ) % prime;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i][index] += 1;

        }
    }

    // Estimate using Carter and Wegman method , using different hash functions
    public double estimateCWS(String key){

        // Get the hashcode of key
        long hashCode = key.hashCode();

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum value of rows
        for (int i = 0; i< depth(); i++){

            // Hashing
            hash = ( (alphas[i]*hashCode)+betas[i] ) % prime;
            if( hash < 0 ) hash = ~hash;

            // Find index
            index = (int) (hash % width());

            min = Math.min(min,sketch[i][index]);
        }

        return min;
    }


    // Update based on polynomial rolling hash function
    public void updatePolyRollingHash(String key){

        // Get the hashCode
        long hashCode = polyRollingHash(key);

        long hash;
        int index;

        // For each row update the hashing cell
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;

            // Convert negative hash
            if( hash < 0 ) hash = ~hash;

            // Modulo
            index = (int) (hash % width());

            // Update the number of collisions
            if( sketch[i-1][index] != 0 ) numOfCollisions++;

            // Update the sketch
            this.sketch[i-1][index] += 1;
        }
    }

    // Estimate using polynomial rolling hash function
    public double estimatePolyRollingHash(String key){

        // Find the index
        long hashCode = polyRollingHash(key);

        long hash;
        int index;

        // Min value
        double min = Double.MAX_VALUE;

        // Find the minimum
        for (int i = 1; i<=this.depth(); i++){

            // Hashing
            hash = hashCode * i;
            if( hash < 0 ) hash = ~hash;

            // Find the index
            index = (int) ((hash) % width());

            min = Math.min(min,sketch[i-1][index]);
        }

        return min;
    }


    // Hash function
    public long hash(String key){

        long hashCode = prime;

        for(int i=0;i<key.length();i++){ hashCode = hashCode*31 + key.charAt(i); }

        return hashCode;
    }

    // Polynomial rolling hash function
    public long polyRollingHash(String key){

        int p = 53;
        int m = (int) (1e9+9);

        long hashCode = 0;
        long power = 1;

        for(int i=0;i<key.length();i++){
            hashCode = (hashCode + (key.charAt(i)-'a'+1)*power) % m;
            power = (power * p) % m;
        }

        return hashCode;
    }


    // Calculations on the depth and width is based on : A Sketch-Based Naive Bayes Algorithms for Evolving Data Streams

    // Calculate the depth = ln(1/1-(1-Δ)^1/m) where (1-Δ)^1/m = (1-δ)^1/m and m = num_classes*num_attr
    public static int calculateDepth(double delta,int m){

        int depth = (int) Math.log(1/(1-Math.pow(1-delta,1.0/m)));

        if( depth == 0 ) return 1;

        return depth;
    }

    // Calculate the width = e*a*N / b*((1+aNE)^1/a-1) where N:total count , b:scaling constant , a:number of attributes and E:epsilon
    public static int calculateWidth(double epsilon,int a,long N,double b){

        double numerator = Math.exp(1) * a * N;
        double denominator = b*(Math.pow(1+a*N*epsilon,1.0/a)-1);

        int width = (int) (numerator/denominator);

        if( width == 0 ) return 1;

        return width;
    }

    // Calculate the width = e*a*N / b*((1+aNE)^1/a-1) where N:total count , b:scaling constant , a:number of attributes and E:epsilon
    public static int calculateWidth(double epsilon,int a,long N,int depth,int sizeParameters){

        double numerator = Math.exp(1) * a * N;
        double denominator = Math.pow(1+a*N*epsilon,1.0/a)-1;

        // Calculate beta
        double b = calculateBeta(numerator,denominator,depth,sizeParameters);

        // Update the denominator
        denominator *= b;

        // Calculate the width
        int width = (int) (numerator/denominator);

        if( width == 0 ) return 1;

        return width;
    }

    // Calculate the beta
    public static double calculateBeta(double numerator,double denominator,int depth,int sizeParameters){

        // Calculate b > numerator*depth / denominator*sizeParameters
        // The constant=1 define the reduction on the size of state(addition)

        // Reduction 35%
        return 2*((numerator*depth) / (denominator*sizeParameters));
    }


    // Reset the sketch.Initialize all entries to zero
    public void reset(){

        for(int i = 0; i<this.depth(); i++){
            for(int j = 0; j<this.width(); j++){
                this.sketch[i][j] = 0d;
            }
        }
    }

    // Return an empty sketch
    public double[][] emptyInstance(){

        // New CountMin
        CountMin newCM = new CountMin(this.depth(),this.width());

        // Reset the newCM
        newCM.reset();

        return newCM.getSketch();
    }

    @Override
    public String toString(){
        return "Depth : " + depth()
               + " , Width : " + width()
               + " , Epsilon : " + epsilon()
               + " , Delta : " + delta()
               + " , Size : " + size()
               + " , Size in bytes : " + sizeBytes();
    }


    // Methods from abstract class VectorType
    // getKey and add method not used ???

    @Override
    public void update(String key) { this.updateMurmur(key); }

    @Override
    public void update(long hashedKey) { this.updateMurmur(hashedKey); }

    @Override
    public double estimate(String key) { return this.estimateMurmur(key); }

    @Override
    public double estimate(long hashedKey) { return this.estimateMurmur(hashedKey); }

    @Override
    public double[][] get() { return this.getSketch(); }

    @Override
    public void set(double[][] sketch){ this.setSketch(sketch); }

    @Override
    public double[][] getEmptyInstance(){ return this.emptyInstance(); }

    @Override
    public void resetVector(){ this.reset(); }

    @Override
    public int getSize(){ return this.size(); }

    @Override
    public void addEntry(String key){}

    @Override
    public void addEntry(long hashedKey){}

    @Override
    public void getKey(){}


}
