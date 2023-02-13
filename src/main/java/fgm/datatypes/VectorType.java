package fgm.datatypes;

import java.io.Serializable;

public abstract class VectorType implements Serializable {

    public abstract void update(String key); // Update the sketch vector using string as key

    public abstract void update(long hashedKey); // Update the sketch vector using long as key

    public abstract double estimate(String key); // Return the estimation of the string-key

    public abstract double estimate(long hashedKey); // Return the estimation of the hashedKey

    public abstract double[][] get(); // Return the sketch vector

    public abstract void set(double[][] sketch); // Set sketch to the sketch vector

    public abstract double[][] getEmptyInstance(); // Return an empty instance(same dimensions with sketch vector)

    public abstract void resetVector(); // Initialize all entries of sketch vector to zero

    public abstract int getSize(); // Return the size of sketch vector

    public abstract void addEntry(String key); // Used only from Vector class

    public abstract void addEntry(long hashedKey); // Used only from Vector class

    public abstract void getKey();

}
