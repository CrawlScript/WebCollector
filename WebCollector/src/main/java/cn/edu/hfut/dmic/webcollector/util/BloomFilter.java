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
package cn.edu.hfut.dmic.webcollector.util;

import java.util.BitSet;

/**
 *
 * @author hu
 */
public class BloomFilter {

    public BitSet bitSet = new BitSet(2 << 24);
    public int seeds[] = {3, 7, 11, 13, 31, 37, 61};

    public BloomFilter() {
    }

    int getHashValue(String str, int n) {
        int result = 0;

        for (int i = 0; i < str.length(); i++) {
            result = seeds[n] * result + (int) str.charAt(i);
            if (result > 2 << 24) {
                result %= 2 << 24;
            }
        }
        return result;
    }

    public boolean contains(String str) {

        for (int i = 0; i < 7; i++) {
            int hash = getHashValue(str, i);
            if (bitSet.get(hash) == false) {
                return false;
            }
        }
        return true;
    }

    public void add(String str) {

        for (int i = 0; i < 7; i++) {
            int hash = getHashValue(str, i);
            bitSet.set(hash, true);
        }
    }
    
    public static void main(String[] args){
        BloomFilter filter=new BloomFilter();
        String baseurl="http://www.baidu.com";
        for(int i=0;i<10000;i++){
            System.out.println(filter.contains(baseurl+i%100));
            filter.add(baseurl+i);
        }
    }
}
