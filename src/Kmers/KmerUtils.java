package Kmers;

import Streams.StreamUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class KmerUtils
{
    private KmerUtils()
    {
        // Private default constructor so objects can't be created since all methods are static
    }

    public static TreeSet<Integer> getCloseKeys(byte[] origkey, int maxDiff)
    {
        TreeSet<Integer> closest = new TreeSet<>();

        List<byte[]> curkeys = new LinkedList<byte[]>();
        curkeys.add(origkey);
        closest.add(key(origkey));

        for (int i = 1; i<= maxDiff; i++)
        {
            LinkedList<byte[]> newkeys = new LinkedList<>();
            for (byte[] ck: curkeys)
            {
                for (int j = 0; j < ck.length; j ++)
                {
                    for (byte k = 0; k < 4; k++)
                    {
                        byte[] newbytes = Arrays.copyOf(ck,ck.length);
                        newbytes[j] = k;
                        int nk = key(newbytes);
                        newkeys.add(newbytes);
                        closest.add(nk);
                    }
                }
            }
            curkeys = newkeys;
        }

        return closest;
    }

    private static int key(byte[] chars)
    {
        int key = 0;
        for (int i = 0; i < chars.length; i++)
        {
            key = key * 4 + chars[i];
        }
        return key;
    }

    public static <D> Stream<KmerWithData<D>> restrictedStream(Stream<KmerWithData<D>> stream,
                                                               int minLength, int maxLength, BinaryOperator<KmerWithData<D>> reducer)
    {
        return StreamUtils.groupAndReduceStream(
                stream.filter(kwd -> kwd.getKmer().length() >= minLength).map(kwd -> kwd.limitTo(maxLength)),
                (kwd1,kwd2) -> kwd1.getKmer().equals(kwd2.getKmer()),
                reducer
        );
    }
}
