package cn.edu.hfut.dmic.dm;

import java.util.*;

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
    public int[] labels;
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

    public void initCenters(){
        centers[0]=vectors[0].clone();
        int centersSize=1;
        Random random=new Random();
        for(int i=1;i<k;i++){

            double[] p=new double[vectors.length];
            double sum=0;
            for(int row=0;row<vectors.length;row++){
                double disSum=0;
                for(int j=0;j<centersSize;j++){
                    disSum+=dis(centers[j],vectors[row]);
                }
                sum+=disSum;
                p[row]=sum;
            }
            for(int row=0;row<vectors.length;row++){
                p[row]=p[row]/sum;
            }
            double r;
            while((r=random.nextDouble())==0);
            for(int row=0;row<vectors.length;row++){
                if(p[row]>=r){
                    centers[centersSize]=vectors[row].clone();
                    centersSize++;
                    break;
                }
            }
        }

    }

    public KMeans(double[][] vectors, int k) {
        this.vectors = vectors;

        this.k = k;
        vectorLen = vectors[0].length;

        centers = new double[k][];

        labelCount = new int[k];
        vectorSum = new double[k][];
        labels = new int[vectors.length];
        for (int i = 0; i < k; i++) {
            vectorSum[i] = new double[vectorLen];
            //centers[i] = vectors[i].clone();
        }
        initCenters();

    }

    public KMeans(ArrayList<HashMap<Integer,Double>> vectorMapList,int vectorLen, int k) {
        this(convertVectorMapList(vectorMapList,vectorLen),k);
    }

    public static double[][] convertVectorMapList(ArrayList<HashMap<Integer,Double>> vectorMapList,int vectorLen){
        double[][] result = new double[vectorMapList.size()][vectorLen];
        for (int row = 0; row < vectorMapList.size(); row++) {
            for (int col = 0; col < vectorLen; col++) {
                result[row][col] = 0;
            }
            for (Map.Entry<Integer, Double> entry : vectorMapList.get(row).entrySet()) {
                result[row][entry.getKey()] = entry.getValue();
            }
        }
        return result;
    }

    public static double[][] convertVectorList(ArrayList<double[]> vectorList) {
        double[][] result = new double[vectorList.size()][];
        for (int row = 0; row < vectorList.size(); row++) {
            result[row] = vectorList.get(row);
        }
        return result;
    }


    public KMeans(ArrayList<double[]> vectorList, int k) {
        this(convertVectorList(vectorList), k);
    }

    public void start(int round) {
        int count = 0;
        while (count++ < round && !once()) ;
    }



    public boolean once() {
        System.out.println("start once");
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
