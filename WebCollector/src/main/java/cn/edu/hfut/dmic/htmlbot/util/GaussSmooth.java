/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.htmlbot.util;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by a on 2014/11/2.
 */
public class GaussSmooth {

    public static double[] getGaussWindow(int r){
        int size=1+r*2;
        double[] window=new double[size];
        for(int i=-r;i<=r;i++){
            window[i+r]=1.0/Math.pow(Math.E,(i*i+0.0)/(2*r*r));

        }
        return window;
    }

    public static ArrayList<Double> gaussSmooth(ArrayList<Double> list,int r){
        double[] window=getGaussWindow(r);
        double wSum=0;
        for(double d:window){
            wSum+=d;
        }
        ArrayList<Double> result=new ArrayList<Double>();
        for(int i=0;i<list.size();i++){
            if(i<r||i>list.size()-1-r){

                result.add(list.get(i));
                continue;
            }
            double sum=0;
            for(int j=-r;j<=r;j++){
                int index=i+j;
                sum+=window[j+r]*list.get(index);
            }
            double value=sum*window[r]/wSum;
            result.add(value);

        }
        return result;

    }

    public static void main(String[] args){
        ArrayList<Double> doubles=new ArrayList<Double>();
        doubles.add(1.0);
        doubles.add(0.0);
        doubles.add(1.0);
        System.out.println(gaussSmooth(doubles,1));
    }

}
