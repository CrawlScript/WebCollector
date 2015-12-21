package cn.edu.hfut.dmic.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by hu on 15-12-21.
 */
public class KMeans {

    protected double dis(double[] v1, double[] v2) {
        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            sum += (v1[i] - v2[i]) * (v1[i] - v2[i]);
        }
        return Math.sqrt(sum);
    }


    protected int k;
    protected int[] labels;
    protected int vectorLen = -1;


    protected double[][] vectors;


    public int computeLabel(double[] vector) {
//        for(int i=0;i<centers.length;i++){
//            for(int j=0;j<vectorLen;j++){
//                System.out.print(centers[i][j]+",");
//            }
//            System.out.println();
//        }
        double minDis = Double.MAX_VALUE;
        int label = -1;
        for (int i = 0; i < centers.length; i++) {
            double dis = dis(vector, centers[i]);
            if (dis < minDis) {
                label = i;
                minDis = dis;
            }
        }
        return label;
    }

    public double[][] centers;
    int[] labelCount;
    double[][] vectorSum;

    public KMeans(double[][] vectors, int k) {
        this.vectors = vectors;

        this.k = k;
        vectorLen = vectors[0].length;

        centers = new double[k][];

        labelCount = new int[k];
        vectorSum = new double[k][];
        for (int i = 0; i < k; i++) {
            vectorSum[i] = new double[vectorLen];
            centers[i] = vectors[i].clone();
        }
        labels = new int[vectors.length];


    }

    public static double[][] convertVectors(ArrayList<double[]> vectorList) {
        double[][] result = new double[vectorList.size()][];
        for (int row = 0; row < vectorList.size(); row++) {
            result[row] = vectorList.get(row);
        }
        return result;
    }


    public KMeans(ArrayList<double[]> vectorList, int k) {
        this(convertVectors(vectorList), k);
    }

    public void start(int round) {
        int count = 0;
        while (count++ < round && !once()) ;
    }



    public boolean once() {

        for (int i = 0; i < k; i++) {
            labelCount[i] = 0;
            for (int j = 0; j < vectorLen; j++) {
                vectorSum[i][j] = 0;
            }
        }

        for (int row = 0; row < vectors.length; row++) {
            double[] vector = vectors[row];
            int label = computeLabel(vector);
            labels[row] = label;
            labelCount[label]++;
            for (int col = 0; col < vectorLen; col++) {
                vectorSum[label][col] += vector[col];
            }
        }
        boolean hasComplete=true;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < vectorLen; j++) {
                double ave = (double) vectorSum[i][j] / labelCount[i];
                if (ave != centers[i][j]) {
                    hasComplete=false;
                }
                centers[i][j]=ave;
            }
        }
        return hasComplete;

    }

    public void showResult(){
        for(int i=0;i<vectors.length;i++){
            System.out.print("label:"+labels[i]+"\t");
            for(int j=0;j<vectorLen;j++){
                System.out.print(vectors[i][j]+",");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        double[][] vectors = new double[][]{
                {1, 2},
                {1, 1.2},
                {4, 1},
                {100, 101},
                {99, 98},
                {97, 87}
        };


        KMeans kMeans = new KMeans(vectors, 2);
        kMeans.start(5);
        kMeans.showResult();

    }


}
