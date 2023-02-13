package operators;

import datatypes.input.Input;
import fgm.sketch.Murmur3;

public class FeatureHashing {

    private static final long prime = 3198622583914487L;

    // Feature hashing
    public static long[] hashingVector(Input input, int size){

        // Create the vector
        // It is advisable to use a power of two as the size of vector
        long[] vector = new long[size];

        // Initialize the vector
        for(int i=0;i<size;i++) vector[i] = 0;

        // Get the values
        String[] values = input.getValue().split(",");

        for (String value: values) {

            // Hashing the value and get the index
            long hash = hashing(value);

            // Get the index
            int index = (int) (hash % size);

            // Update the vector
            vector[index] += 1;

        }

        return vector;

    }

    // Limit the effect of hash collisions(use two hash functions)
    // Paper : Feature Hashing for Large Scale Multitask Learning
    public static long[] hashingVectorized(Input input, int size){

        // Create the vector
        // It is advisable to use a power of two as the size of vector
        long[] vector = new long[size];

        // Initialize the vector
        for(int i=0;i<size;i++) vector[i] = 0;

        // Get the values
        String[] values = input.getValue().split(",");

        for (String value: values) {

            // Hashing the value
            long hash = hashing(value);

            // Get the index
            int index = (int) (hash % size);

            // Use the another hash function to update the vector => Limit the effect of hash collisions
            if( hash % 2 == 1 ) vector[index] += 1;
            else{
                if(vector[index] != 0) vector[index] -= 1;
            }

        }

        return vector;
    }

    // Hash the value,using Murmur3 hash function
    public static long hashing(String value){

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(value.getBytes());
        long hash2 = (hash >>> 32);
        long hashing;

        // Hashing the value
        hashing = hash + (prime*hash2);

        // Negative value => Flip all the bits
        if( hashing < 0 ) hashing = ~hashing;

        // Return the index
        return hashing;
    }

    // Feature Hashing and limitation the effect of hash collisions
    public static void hashingVectorized(String value,int size,long[] vector){

        // Split up 64-bit hashcode into two 32-bit hashCodes
        long hash = Murmur3.hash64(value.getBytes());
        long hash2 = (hash >>> 32);
        long hashing;

        // Hashing the value
        hashing = hash + (prime*hash2);

        // Negative value => Flip all the bits
        if( hashing < 0 ) hashing = ~hashing;

        // Get the index
        int index = (int) (hashing % size);

        // Use the another hash function to update the vector => Limit the effect of collisions
        if( hashing % 2 == 1 ){ vector[index] += 1; }
        else{
            if(vector[index] != 0) vector[index] -= 1;
        }
    }

}
