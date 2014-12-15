package util;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

/**
 * Created by andrew on 14-12-10.
 *
 * 博客请参考:http://www.cnblogs.com/allensun/archive/2011/02/16/1956532.html
 * 中间遇到许多该算法的基础知识
 * 参考github(基本照抄)的原地址:https://github.com/MagnusS/Java-BloomFilter/blob/master/src/com/skjegstad/utils/BloomFilter.java
 *
 */
public class BloomFilter<E> implements Serializable {
    private BitSet bitSet;                              //布隆过滤器的实体
    private int bitSetSize;                             //过滤器的大小
    private double bitsPerElement;                      //
    private int expectedNumberOffFilterElements;        //期望(最大数量)的要添加的元素数量
    private int numberOffElements;                      //实际添加的元素数量
    private int k;                                      //哈希函数的数量

    static final Charset charset = Charset.forName("UTF-8");

    static final String hashName = "MD5";
    static final MessageDigest digestFunction;

    static {
        MessageDigest tmp;
        try {
            tmp = MessageDigest.getInstance(hashName);
        } catch (NoSuchAlgorithmException e) {
            tmp = null;
        }

        digestFunction = tmp;
    }

    /**
     * @param c 代表平均每个元素所占的bit
     * @param n 代表允许添加元素的最大数量
     * @param k 布隆过滤器的哈希函数的数量
     */

    public BloomFilter(double c, int n, int k) {
        bitsPerElement = c;
        expectedNumberOffFilterElements = n;
        this.k = k;
        bitSetSize = (int) Math.ceil(c * n);
        numberOffElements = 0;
        bitSet = new BitSet(bitSetSize);
    }

    /**
     * @param bitSetSize                      布隆过滤器的bit大小
     * @param expectedNumberOffFilterElements 能够加入过滤器元素的最大个数
     */

    public BloomFilter(int bitSetSize, int expectedNumberOffFilterElements) {
        this(bitSetSize / (double) expectedNumberOffFilterElements,
                expectedNumberOffFilterElements,
                (int) Math.round((bitSetSize / (double) expectedNumberOffFilterElements) * Math.log(2.0)));
    }

    /**
     * @param falsePositiveProbability        期望的出错的概率
     * @param expectedNumberOffFilterElements 过滤器能添加的元素最大数量
     */

    public BloomFilter(double falsePositiveProbability, int expectedNumberOffFilterElements) {
        this(Math.ceil(-Math.log(falsePositiveProbability) / Math.log(2.0)) / Math.log(2.0),    //c = k/ln(2)
                expectedNumberOffFilterElements,
                (int) Math.ceil(-Math.log(falsePositiveProbability) / Math.log(2.0)));           //k = -ln(p)/ln2

    }

    /**
     * @param bitSetSize                      ....
     * @param expectedNumberOffFilterElements ...
     * @param actualNumberOffFilterElements   过滤器里所含元素数量
     * @param filterData
     */

    public BloomFilter(int bitSetSize, int expectedNumberOffFilterElements, int actualNumberOffFilterElements, BitSet filterData) {
        this(bitSetSize, expectedNumberOffFilterElements);
        bitSet = filterData;
        numberOffElements = actualNumberOffFilterElements;
    }

    /**
     * @param val     传入数据的值
     * @param charset 字符编码
     */

    public static int createHash(String val, Charset charset) {
        return createHash(val.getBytes(charset));
    }

    /**
     * @param val
     */

    public static int createHash(String val) {
        return createHash(val, charset);
    }

    public static int createHash(byte[] data) {
        return createHashes(data, 1)[0];
    }

    public static int[] createHashes(byte[] data, int hashes) {
        int[] result = new int[hashes];

        int k = 0;
        byte salt = 0;
        while (k < hashes) {
            byte[] digest;

            synchronized (digestFunction) {
                digestFunction.update(salt);
                salt++;
                digest = digestFunction.digest(data);
            }

            for (int i = 0; i < digest.length / 4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i * 4); j < (i * 4) + 4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final BloomFilter<E> other = (BloomFilter<E>) obj;

        if (this.numberOffElements != other.numberOffElements)
            return false;
        if (this.k != other.k)
            return false;
        if (this.bitSetSize != other.bitSetSize)
            return false;
        if (this.bitSet != other.bitSet && (this.bitSet == null || this.bitSet.equals(other.bitSet)))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = hash * 61 + (this.bitSet != null ? this.bitSet.hashCode() : 0);
        hash = hash * 61 + (this.expectedNumberOffFilterElements);
        hash = hash * 61 + this.bitSetSize;
        hash = 61 * hash + this.k;
        return hash;
    }

    public double expectedFalsePositiveProbability() {
        return getFalsePositiveProbability(expectedNumberOffFilterElements);
    }

    /**
     * p = (1 - e ^ (-k * n / m) ^ k)
     *
     * @param numberOffElements
     * @return
     */
    public double getFalsePositiveProbability(int numberOffElements) {
        return Math.pow(1 - Math.exp(-k * numberOffElements / (double) bitSetSize), k);
    }

    /**
     * @return
     */
    public double getFalsePositiveProbability() {
        return getFalsePositiveProbability(numberOffElements);
    }

    /**
     * @return
     */
    public int getK()
    {
        return k;
    }

    /**
     * 设置过滤器所有位为false
     * 并将numberoffElements设置为0
     */
    public void clear()
    {
        bitSet.clear();
        numberOffElements = 0;
    }

    /**
     *
     * @param element
     */
    public void add(E element)
    {
        add(element.toString().getBytes(charset));
    }

    /**
     *
     * @param datas
     */
    public void add(byte[] datas)
    {
        int[] hashes = createHashes(datas, k);
        for (int hash :hashes)
        {
            bitSet.set(Math.abs(hash % bitSetSize), true);
        }
        numberOffElements++;
    }

    /**
     *
     * @param c
     */
    public void addAll(Collection<? extends E> c)
    {
        for (E element : c)
        {
            add(element);
        }
    }

    /**
     *
     * @param element
     * @return
     */
    public boolean contains(E element)
    {
        return contains(element.toString().getBytes(charset));
    }

    /**
     *
     * @param bytes
     * @return
     */
    public boolean contains(byte[] bytes)
    {
        int[] hashes = createHashes(bytes,k);
         for (int hash : hashes)
         {
             if (!bitSet.get(Math.abs(hash % bitSetSize)))
                 return false;
         }
        return true;
    }

    /**
     *
     * @param c
     * @return 有不包含，返回false
     */
    public boolean containsAll(Collection<? extends E> c)
    {
        for (E element : c)
        {
            if (!contains(element))
                return false;
        }
        return true;
    }

    /**
     *
     * @param bit
     * @return
     */
    public boolean getBit(int bit)
    {
        return bitSet.get(bit);
    }

    /**
     *
     * @param bit
     * @param value
     */
    public void setBit(int bit, boolean value)
    {
        bitSet.set(bit, value);
    }

    /**
     *
     * @return
     */
    public BitSet getBitSet()
    {
        return bitSet;
    }

    /**
     *
     * @return
     */
    public int size()
    {
        return bitSetSize;
    }

    /**
     *
     * @return
     */
    public int count()
    {
        return numberOffElements;
    }

    /**
     *
     * @return
     */
    public int getExpectedNumberOffFilterElements()
    {
        return expectedNumberOffFilterElements;
    }

    /**
     *
     * @return
     */
    public double getExpectedBitsPerElement()
    {
        return bitsPerElement;
    }

    /**
     *
     * @return
     */
    public double getBitsPerElement()
    {
        return bitSetSize / (double)numberOffElements;
    }
}


