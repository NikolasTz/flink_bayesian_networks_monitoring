package utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class Util {

    // *************************************************************************
    //                              ArrayList UTILS
    // *************************************************************************

    public static <T> String convertToString(List<T> list){
        if( list == null ){ return ""; }
        return list.toString().replaceAll("[\\[\\]]","");
    }

    // Find the index of list which correspond to input
    public static <T> int findIndex(List<T> list,T input){

        int index = -1;
        if( list == null || input == null) return index;

        for(int j=0;j<list.size();j++){
            if(list.get(j).equals(input)){
                return j;
            }
        }

        return index;

    }

    // Check two lists if they are equals
    public static <T> boolean equalsList(List<T> list1,List<T> list2){

        if( list1 == null && list2 == null ) return true;
        else if( list1 != null && list2 == null ) return false;
        else if( list1 == null ) return false;

        if( list1.size() != list2.size() ) return false;
        else{
            for(int i=0;i<list1.size();i++){
                if( !list1.get(i).equals(list2.get(i)) ) return false;
            }
        }

        return true;
    }

    // Get the size of list in bytes
    public static <T> int getSizeList(List<T> list) throws ClassNotFoundException {

        int size = 0;

        // Corner case
        if( list == null ) return size;

        // Get the type

        // String class
        if ( Class.forName("java.lang.String").isInstance(list.get(0)) ){

            // Get the size
            for (T t : list) { size += t.toString().length() * Character.BYTES; }
        }

        // Integer class
        if ( Class.forName("java.lang.Integer").isInstance(list.get(0)) ){

            // Get the size
            size = list.size() * Integer.BYTES;
        }

        // Long class
        if ( Class.forName("java.lang.Long").isInstance(list.get(0)) ){

            // Get the size
            size = list.size() * Long.BYTES;
        }

        // Double class
        if ( Class.forName("java.lang.Double").isInstance(list.get(0)) ){

            // Get the size
            size = list.size() * Double.BYTES;
        }

        // Float class
        if ( Class.forName("java.lang.Float").isInstance(list.get(0)) ){

            // Get the size
            size = list.size() * Float.BYTES;
        }

        return size;
    }


    // *************************************************************************
    //                             Prime numbers
    // *************************************************************************

    // Generate prime numbers using the algorithm Sieve of Eratosthenes
    public static void generatePrimes(int size){

        boolean[] prime = new boolean[size + 1];
        for (int i = 0; i <= size; i++)
            prime[i] = true;

        for (int p = 2; p * p <= size; p++) {
            // If prime[p] is not changed, then it is a
            // prime
            if (prime[p]) {
                // Update all multiples of p
                for (int i = p * p; i <= size; i += p)
                    prime[i] = false;
            }
        }

        // Print all prime numbers
        for (int i = 2; i <= size; i++) {
            if (prime[i])
                System.out.print(i + " ");
        }
    }

    // Check if the number n is prime
    public static boolean isPrime(int n) {

        // Corner cases
        if (n <= 1) return false;
        if (n <= 3) return true;

        // This is checked so that we can skip middle five numbers in below loop
        if (n % 2 == 0 || n % 3 == 0) return false;

        for (int i = 5; i * i <= n; i = i + 6)
            if (n % i == 0 || n % (i + 2) == 0) return false;

        return true;
    }

    // Generate the smallest prime number that is greater than n
    public static int nextPrime(int n) {

        // Base case
        if (n <= 1) return 2;

        int prime = n;
        boolean found = false;

        // Loop continuously until isPrime returns true
        while (!found) {
            prime++;
            if (isPrime(prime)) found = true;
        }

        return prime;
    }

    // Returns a pseudorandom, uniformly distributed int value between 0 (exclusive) and the specified value (exclusive)
    public static int nextInt(int n){

        // Corner case
        if( n < 0 ) return 1;

        Random rand = new Random();
        int ret = 0;
        while( ret == 0 ){ ret = rand.nextInt(n); }
        return ret;
    }


    // *************************************************************************
    //                             Conversion Utils
    // *************************************************************************

    /** Convert a long to byte array based Big Endian system
     * @param value The long number
     * @return The byte array
     */
    public static byte[] longToBytesBE(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    /** Convert a long to byte array based Little Endian system
     * @param value The long number
     * @return The byte array
     */
    public static byte[] longToByteArrayLE(long value) {
        return new byte[]{(byte) (value),
                (byte) (value >> 8),
                (byte) (value >> 16),
                (byte) (value >> 24),
                (byte) (value >> 32),
                (byte) (value >> 40),
                (byte) (value >> 48),
                (byte) (value >> 56),};
    }


    // *************************************************************************
    //                             Encode Utils
    // *************************************************************************

    // Encode the argument path using UTF-8 encoding
    public static String encode(String path) throws UnsupportedEncodingException {
        return URLEncoder.encode(path,"UTF-8");
    }

}
