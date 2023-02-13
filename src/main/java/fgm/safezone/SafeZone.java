package fgm.safezone;

import config.FGMConfig;

import static utils.MathUtils.*;

public class SafeZone {

    // We focus on Fp moments and specifically on F1 moment that is the SUM or COUNT
    // So use safe functions(phi) of the following form : ||x+E||p - T , where E: estimated global state vector,x: drift vector and T: threshold
    // The threshold compute based on type of monitoring that is one-shot or continuous monitoring query
    // We interested again to the continuous monitoring query
    // So the threshold T has got the following format : T = (1+epsilon)||E||p , where epsilon : Define the accuracy of query  and changes in each round
    // In case of F1 : phi = ||x+E|| - (1+epsilon)||E|| , where p=1
    // Note that selecting not to raise the norm to p yields better quiescent regions

    private double epsilon; // The accuracy of query

    // Constructors
    public SafeZone(){}
    public SafeZone(double epsilon){ this.epsilon = epsilon; }
    public SafeZone(FGMConfig config){

        // Get the epsilon
        epsilon = config.epsilon();
    }


    // Getters
    public double getEpsilon() { return epsilon; }

    // Setters
    public void setEpsilon(double epsilon) { this.epsilon = epsilon; }


    // Methods

    // Continuous query

    // Calculate the phi(Xi) using vectors
    public double safeFunction(double[] driftVector,double[] estimatedVector){

        // Calculate the phi(Xi)
        double xe = norm(addVectors(driftVector,estimatedVector),1); // ||x+E||
        double threshold = (1+this.epsilon)*norm(estimatedVector,1); // (1+epsilon)||E||

        return xe-threshold;
    }

    // Calculate the phi(Xi) using arrays
    public double safeFunction(double[][] driftVector,double[][] estimatedVector){

        // Convert the arrays to vector using the row major method
        double[] convertedDrift = rowMajor(driftVector);
        double[] convertedEstimated = rowMajor(estimatedVector);

        // Find min value of estimatedVector
        double min = minArray(convertedEstimated);
        if( min == 0 ) min = 1d;

        // Calculate the phi(Xi)
        double minViolatedThreshold = this.epsilon*min;
        double normEstimatedVector = norm(convertedEstimated,1);
        double normMinViolatedEstimatedVector = normEstimatedVector + minViolatedThreshold;
        double coefficientError = normMinViolatedEstimatedVector/normEstimatedVector;
        // System.out.println("Coefficient error : "+coefficientError);

        double xe = norm(addVectors(convertedDrift,convertedEstimated),1); // ||x+E||
        double threshold = coefficientError*normEstimatedVector; // (1+epsilon)||E||

        // Calculate the phi(Xi)
        // double xe = norm(addVectors(convertedDrift,convertedEstimated),1); // ||x+E||
        // double threshold = (1+this.epsilon)*norm(convertedEstimated,1); // (1+epsilon)||E||

        return xe-threshold;
    }


    // One shot-query

    // Calculate the phi(Xi) using vectors
    public double safeFunction(double[] driftVector,double[] estimatedVector,double threshold){

        // Calculate the phi(Xi)
        double xe = norm(addVectors(driftVector,estimatedVector),1); // ||x+E||

        return xe-threshold; // ||x+E|| - threshold
    }

    // Calculate the phi(Xi) using arrays
    public double safeFunction(double[][] driftVector,double[][] estimatedVector,double threshold){

        // Convert the arrays to vector using the row major method
        double[] convertedDrift = rowMajor(driftVector);
        double[] convertedEstimated = rowMajor(estimatedVector);

        // Calculate the phi(Xi)
        double xe = norm(addVectors(convertedDrift,convertedEstimated),1); // ||x+E||

        return xe-threshold; // ||x+E|| - threshold
    }


    @Override
    public String toString(){ return "Safe zone : epsilon : " + this.epsilon; }


}
