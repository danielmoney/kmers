package Database;

import Concurrent.LimitedQueueExecutor;
import Concurrent.LimitedQueueExecutor2;
import DataTypes.DataType;
import Exceptions.InvalidBaseException;
import KmerFiles.KmerFile;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Kmers.KmerWithDataStreamWrapper;
import Streams.StreamUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.DataFormatException;

public class DB<D>
{
    public DB(List<KmerFile<D>> files) //, /*BinaryOperator<D> mergeDataFunction*/ DataType<?,D> dataType)
    {
        // Stop this being modified after creation by creating a new copy as we do some checks on the files at construction
        this.files = new LinkedList<>(files);
//        this.merge = mergeDataFunction;
//        this.dataType = dataType;

        this.dataType = files.get(0).getDataType();
        // Check all files are the same!

        if (files.isEmpty())
        {
            // Throw an error!!
        }

        // We need to get these from the files!!
        this.minLength = files.get(0).getMinLength();
        this.maxLength = files.get(0).getMaxLength();
        this.keyLength = files.get(0).getKeyLength();
        // We can probably cope with key length of different length but can add that in later if needed.

        for (KmerFile<D> f: files)
        {
            if ((f.getMinLength() != minLength) || (f.getMaxLength() != maxLength)  || (f.getKeyLength() != keyLength))
            {
                // Throw an error!
            }
        }
    }


//    public <S> Stream<ClosestInfo<S,D>> getNearestKmers(Stream<KmerWithData<S>> searchKmers, int maxDiff, boolean just)
//    {
//        return StreamUtils.groupedStream(searchKmers, (kwd1, kwd2) -> kwd1.getKmer().key(keyLength) == kwd2.getKmer().key(keyLength), Collectors.toList())
//                .map(l -> processNearestCommonKey(l, maxDiff, just).stream()).flatMap(s -> s);
//    }

    public <S> Stream<ClosestInfo<S,D>> getNearestKmers(KmerWithDataStreamWrapper<S> searchKmers, int maxDiff, boolean just)
    {
        if ((searchKmers.getMinLength() == searchKmers.getMaxLength()) && (maxDiff == 0)) //Quick match
        {
            return StreamUtils.matchTwoStreams(searchKmers.stream(),
//                    KmerUtils.restrictedStream(allKmers(),searchKmers.getMinLength(),searchKmers.getMaxLength(), merge),
                    KmerUtils.restrictedStream(allKmers(),searchKmers.getMinLength(),searchKmers.getMaxLength(), dataType).stream(),
                    (kwd1, kwd2) -> new ClosestInfo<>(kwd1,kwd2),
                    (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()));
        }
        else
        {
            Stream<List<KmerWithData<S>>> groupedStream = StreamUtils.groupedStream(searchKmers.stream(), (kwd1, kwd2) -> kwd1.getKmer().key(keyLength) == kwd2.getKmer().key(keyLength), Collectors.toList());
            return StreamSupport.stream(new ProcessCommonSpliterator<>(groupedStream, maxDiff, just), false).flatMap(l -> l.stream());
        }
    }

    public <S> List<ClosestInfo<S,D>> getNearestKmers(List<KmerWithData<S>> kmers, int maxDiff, boolean just) throws InvalidBaseException, IOException, DataFormatException
    {
        return  getNearestKmers(kmers, maxDiff, just, false);
    }

    public <S> List<ClosestInfo<S,D>> getNearestKmers(List<KmerWithData<S>> kmers, int maxDiff, boolean just, boolean sorted) throws InvalidBaseException, IOException, DataFormatException
    {
        //Map<KmerWithData<S>, ClosestInfo<S,D>> ret = new TreeMap<>();
        List<ClosestInfo<S,D>> ret = new ArrayList<>();

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
                //ret.putAll(processNearestCommonKey(commonKey, maxDiff, just));
                ret.addAll(processNearestCommonKey(commonKey, maxDiff, just));
                curkey = k.getKmer().key(keyLength);
                commonKey = new LinkedList<>();
            }
            commonKey.add(k);
        }
        //ret.putAll(processNearestCommonKey(commonKey,maxDiff,just));
        ret.addAll(processNearestCommonKey(commonKey, maxDiff, just));

        //return new LinkedList<>(ret.values());
        return ret;
    }

    public KmerWithDataStreamWrapper<D> kmers(int key)
    {
        // Don't need to check that db streams have same min/max length as we should check that at db creation
        return new KmerWithDataStreamWrapper<>(
                StreamUtils.mergeSortedStreams(files.stream().map(f -> f.kmers(key).stream()).collect(Collectors.toList()),
                (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
                minLength,
                maxLength);
    }

    public KmerWithDataStreamWrapper<D> allKmers()
    {
        // Don't need to check that db streams have same min/max length as we should check that at db creation
        return new KmerWithDataStreamWrapper<>(
                StreamUtils.mergeSortedStreams(files.stream().map(f -> f.allKmers().stream()).collect(Collectors.toList()),
                        (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
                minLength,
                maxLength);
    }

    //private <S> Map<KmerWithData<S>, ClosestInfo<S,D>> processNearestCommonKey(List<KmerWithData<S>> kmers, int maxDiff, boolean just)
    private <S> List<ClosestInfo<S,D>> processNearestCommonKey(List<KmerWithData<S>> kmers, int maxDiff, boolean just)
    {
//        Map<KmerWithData<S>, ClosestInfo<S,D>> currentBest = new TreeMap<>();
        List<ClosestInfo<S,D>> currentBest = new ArrayList<>(kmers.size());

        for (KmerWithData<S> k: kmers)
        {
            //currentBest.put(k, new ClosestInfo<S,D>(k,new HashMap<>(), maxDiff));
            currentBest.add(new ClosestInfo<S,D>(k,new HashMap<>(), maxDiff));
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
//            Root<D> r = new Root<>(maxLength,minLength,merge);
            Root<D> r = new Root<>(maxLength,minLength,dataType);
//                System.out.println(key);
            for (KmerFile<D> f: files)
            {
                f.kmers(key).stream().forEach(k -> r.addKmer(k));
            }
            //for (KmerWithData<S> k : kmers)
            for (int i = 0; i < kmers.size(); i++)
            {
                KmerWithData<S> k = kmers.get(i);
                ClosestInfo<S,D> newci = r.closestKmers(k, maxDiff, just);
                //ClosestInfo<S,D> oldci = currentBest.get(k);
                ClosestInfo<S,D> oldci = currentBest.get(i);
                if ((newci.getMinDist() == oldci.getMinDist()) || !just)
                {
                    oldci.merge(newci);
                }
                if ((newci.getMinDist() < oldci.getMinDist()) && just)
                {
                    //currentBest.put(k, newci);
                    currentBest.set(i,newci);
                }
            }
         }

        return currentBest;
    }

    private class ProcessCommonSpliterator<S> implements Spliterator<List<ClosestInfo<S,D>>>
    {
        private ProcessCommonSpliterator(Stream<List<KmerWithData<S>>> inputStream, int maxDiff, boolean just)
        {
            this.input = inputStream.iterator();
            ex = new LimitedQueueExecutor2<List<ClosestInfo<S,D>>>(7,1);
            futures = new LinkedList<>();
            for (int i = 0; i < 8; i++)
            {
                if (input.hasNext())
                {
                    List<KmerWithData<S>> l = input.next();
                    futures.add(ex.submit(() -> processNearestCommonKey(l, maxDiff, just)));
                }
            }
            this.maxDiff = maxDiff;
            this.just = just;
        }

        public boolean tryAdvance(Consumer<? super List<ClosestInfo<S,D>>> consumer)
        {
            if (!futures.isEmpty())
            {
                try
                {
                    Future<List<ClosestInfo<S,D>>> future = futures.poll();
                    if (input.hasNext())
                    {
                        List<KmerWithData<S>> l = input.next();
                        futures.add(ex.submit(() -> processNearestCommonKey(l, maxDiff, just)));
                    }
                    else
                    {
                        ex.shutdown();
                    }
                    consumer.accept(future.get());
                }
                catch (InterruptedException e)
                {
                    return false;
                }
                catch (ExecutionException e)
                {
                    return false;
                }
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<List<ClosestInfo<S,D>>> trySplit()
        {
            return null;
        }

        @Override
        public long estimateSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.IMMUTABLE | Spliterator.ORDERED;
        }

        LinkedList<Future<List<ClosestInfo<S,D>>>> futures;
        Iterator<List<KmerWithData<S>>> input;
        LimitedQueueExecutor2<List<ClosestInfo<S,D>>> ex;
        private int maxDiff;
        private boolean just;
    }

//    private BiConsumer<D,D> merge;
    private DataType<?,D> dataType;
    private int keyLength;
    private int minLength;
    private int maxLength;
    private List<KmerFile<D>> files;
}
