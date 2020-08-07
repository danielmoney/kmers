package Database;

import Exceptions.InvalidBaseException;
import KmerFiles.KmerFile;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Streams.StreamUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;

public class DB<D>
{
    public DB(List<KmerFile<D>> files, int keyLength, BiFunction<D,D,D> mergeDataFunction, int minLength, int maxLength)
    {
        this.files = files;
        this.keyLength = keyLength;
        this.merge = mergeDataFunction;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }


    public <S> Stream<ClosestInfo<S,D>> getNearestKmers(Stream<KmerWithData<S>> searchKmers, int maxDiff, boolean just)
    {
        return StreamUtils.groupedStream(searchKmers, (kwd1, kwd2) -> kwd1.getKmer().key(keyLength) == kwd2.getKmer().key(keyLength), Collectors.toList())
                .map(l -> processNearestCommonKey(l, maxDiff, just).values().stream()).flatMap(s -> s);
    }

    public <S> List<ClosestInfo<S,D>> getNearestKmers(List<KmerWithData<S>> kmers, int maxDiff, boolean just) throws InvalidBaseException, IOException, DataFormatException
    {
        return  getNearestKmers(kmers, maxDiff, just, false);
    }

    public <S> List<ClosestInfo<S,D>> getNearestKmers(List<KmerWithData<S>> kmers, int maxDiff, boolean just, boolean sorted) throws InvalidBaseException, IOException, DataFormatException
    {
        Map<KmerWithData<S>, ClosestInfo<S,D>> ret = new TreeMap<>();

        if (!sorted)
        {
            Collections.sort(kmers);
        }

        int curkey = kmers.get(0).getKmer().key(keyLength);
        List<KmerWithData<S>> commonKey = new LinkedList<>();

        for (KmerWithData<S> k: kmers)
        {
            if (k.getKmer().key(keyLength) != curkey)
            {
                ret.putAll(processNearestCommonKey(commonKey, maxDiff, just));
                curkey = k.getKmer().key(keyLength);
                commonKey = new LinkedList<>();
            }
            commonKey.add(k);
        }
        ret.putAll(processNearestCommonKey(commonKey,maxDiff,just));

        return new LinkedList<>(ret.values());
    }


    private <S> Map<KmerWithData<S>, ClosestInfo<S,D>> processNearestCommonKey(List<KmerWithData<S>> kmers, int maxDiff, boolean just)
    {
        Map<KmerWithData<S>, ClosestInfo<S,D>> currentBest = new TreeMap<>();

        for (KmerWithData<S> k: kmers)
        {
            currentBest.put(k, new ClosestInfo<S,D>(k,new HashMap<>(), maxDiff));
        }

        byte[] keybytes = new byte[keyLength];
        System.arraycopy(kmers.get(0).getKmer().getRawBytes(),0,keybytes,0,keyLength);
        TreeSet<Integer> closekeys = KmerUtils.getCloseKeys(keybytes, maxDiff);

        /**************************************************************************
         * ************************************************************************
         * Should we paralize here? or maybe in above function?
         * Current thought is above but want to think about more...
         * ************************************************************************
         **************************************************************************/


        for (int key: closekeys)
        {
            Root<D> r = new Root<>(maxLength,minLength,merge);
//                System.out.println(key);
            for (KmerFile<D> f: files)
            {
                f.kmers(key).forEach(k -> r.addKmer(k));
            }
            for (KmerWithData<S> k : kmers)
            {
                ClosestInfo<S,D> ci = r.closestKmers2(k, maxDiff, just);
                if ((ci.getMinDist() == currentBest.get(k).getMinDist()) || !just)
                {
                    currentBest.get(k).merge(ci);
                }
                if ((ci.getMinDist() < currentBest.get(k).getMinDist()) && just)
                {
                    currentBest.put(k, ci);
                }
            }
         }

        return currentBest;
    }

    private BiFunction<D,D,D> merge;
    private int keyLength;
    private int minLength;
    private int maxLength;
    private List<KmerFile<D>> files;
}
