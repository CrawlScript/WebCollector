package cn.edu.hfut.dmic.dm;

/**
 * Created by hu on 15-12-21.
 */
public class KMeans {

    protected double dis(double[] v1,double[] v2){
        double sum=0;
        for(int i=0;i<v1.length;i++){
            sum+=(v1[i]-v2[i])*(v1[i]-v2[i]);
        }
        return Math.sqrt(sum);
    }


    protected int k;


    protected double[][] vectors;

    {
        int row=5;
        int col=2;
        vectors=new double[row][col];
        for(int r=0;r<row;r++){
            for(int c=0;c<col;c++){
                vectors[r][c]=r;
            }
        }

    }

    public double[][] centors;


    public KMeans(double[][] vectors,int k){
        this.vectors=vectors;
        this.k=k;

        centors=new double[k][];
        for(int i=0;i<k;i++){
            centors[i]=vectors[i];
        }
    }

    public void once(){

    }





}
