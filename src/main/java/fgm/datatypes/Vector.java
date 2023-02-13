package fgm.datatypes;


import fgm.sketch.Murmur3;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.MathUtils.convert2D;
import static utils.MathUtils.rowMajor;

public class Vector extends VectorType {

    // External hash function used on project is xxh3 hash function for generating the key
    // Internal hash function used from this class is Murmur3 hash function
    // For function estimate,update and addEntry with argument String key will be used Murmur3 hash function to hashing the key

    // Key is the hash-coded value of parameter key
    private LinkedHashMap<Long, Double> vector;

    // Constructors
    public Vector(){ vector = new LinkedHashMap<>(); }

    // Getters
    public LinkedHashMap<Long, Double> getVector() { return vector; }

    // Setters
    public void setVector(LinkedHashMap<Long, Double> vector) { this.vector = vector; }

    // Methods
    public double[] vector(){
        return Arrays.stream(vector.values().toArray(new Double[0])).mapToDouble(Double::doubleValue).toArray();
    }
    public int size(){ return vector.size(); }
    public int sizeBytes(){ return vector.size()*(Long.BYTES+Double.BYTES); }


    // Methods from abstract class VectorType
    // getKey method not used ???

    @Override
    public void update(String key){
        long hashedKey = Murmur3.hash64(key.getBytes());
        this.vector.compute(hashedKey,(k, v) -> (v == null) ? 0 : v+1);
    }

    @Override
    public void update(long hashedKey){ this.vector.compute(hashedKey,(k, v) -> (v == null) ? 0 : v+1); }

    @Override
    public double estimate(String key) {
        long hashedKey = Murmur3.hash64(key.getBytes());
        return this.vector.get(hashedKey);
    }

    @Override
    public double estimate(long hashedKey) { return this.vector.get(hashedKey); }

    @Override
    public double[][] get() { return convert2D(this.vector(),true,1,this.vector.size()); }

    @Override
    public void set(double[][] sketch){

        // Convert vector to array
        double[] values = rowMajor(sketch);
        AtomicInteger index = new AtomicInteger();

        // Update the linkedHashMap
        this.vector.replaceAll( (k,v) -> {
            v = values[index.get()];
            index.set(index.get() + 1);
            return v;
        });
    }

    @Override
    public double[][] getEmptyInstance(){

        double[][] empty = this.get();
        for (double[] row : empty) Arrays.fill(row,0d);
        return empty;
    }

    @Override
    public int getSize(){ return this.size(); }

    @Override
    public void addEntry(String key){
        long hashedKey = Murmur3.hash64(key.getBytes());
        this.vector.put(hashedKey,0d);
    }

    @Override
    public void addEntry(long hashedKey){ this.vector.put(hashedKey,0d); }

    @Override
    public void resetVector(){ this.vector.replaceAll((k,v) -> v = 0d); }

    @Override
    public void getKey(){}

    @Override
    public String toString(){ return vector.toString()
                                     + " , size : " + this.size();
    }


}
