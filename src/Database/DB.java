package Database;

import Concurrent.LimitedQueueExecutor2;
import DataTypes.MergeableDataType;
import KmerFiles.KmerFile;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Streams.StreamUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DB<D>
{
    public DB(List<KmerFile<D>> files)
    {
        // Stop this being modified after creation by creating a new copy as we do some checks on the files at construction
        this.files = new LinkedList<>(files);

        this.dataType = files.get(0).getDataType();
        for (KmerFile<D> f: files)
        {
            // Check for relevant simialrity and throw error
        }

        if (files.isEmpty())
        {
            // Throw an error!!
        }

        // We need to get these from the files!!
        this.minLength = files.get(0).getMinLength();
        this.maxLength = files.get(0).getMaxLength();
        this.keyLength = files.get(0).getKeyLength();
        // We can probably cope with key length of different length but can add that in later if needed.
        // Probably also different min / maes by using min of the maxes and the max of the mins and the filtering
        // when getting the kmers.  This can be a later enchacement!

        for (KmerFile<D> f: files)
        {
            if ((f.getMinLength() != minLength) || (f.getMaxLength() != maxLength)  || (f.getKeyLength() != keyLength)
                    || !f.getRC())
            {
                // Throw an error!
            }
        }
    }

    public <S> Stream<ClosestInfo<S,D>> getNearestKmers(KmerStream<S> searchKmers, int maxDiff, boolean just)
    {
        if ((searchKmers.getMinLength() < minLength) || (searchKmers.getMaxLength() > maxLength))
        {
            // Throw an error
        }

        if ((searchKmers.getMinLength() == searchKmers.getMaxLength()) && (maxDiff == 0)) //Quick match
        {
            return StreamUtils.matchTwoStreams(searchKmers.onlyStandard().stream(),
                    KmerUtils.restrictedStream(allKmers(),searchKmers.getMinLength(),searchKmers.getMaxLength(), dataType).stream(),
                    (kwd1, kwd2) -> new ClosestInfo<>(kwd1,kwd2),
                    (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()));
        }
        else
        {
            Stream<List<KmerWithData<S>>> groupedStream = StreamUtils.groupedStream(searchKmers.onlyStandard().stream(), (kwd1, kwd2) -> kwd1.getKmer().key(keyLength) == kwd2.getKmer().key(keyLength), Collectors.toList());
            return StreamSupport.stream(new ProcessCommonSpliterator<>(groupedStream, maxDiff, just), false).flatMap(l -> l.stream());
        }
    }

    public KmerStream<D> kmers(int key)
    {
        // Don't need to check that db streams have same min/max length as we should check that at db creation
        return new KmerStream<>(
                StreamUtils.mergeSortedStreams(files.stream().map(f -> f.kmers(key).stream()).collect(Collectors.toList()),
                (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
                minLength,
                maxLength,
                true);
    }

    public KmerStream<D> allKmers()
    {
        // Don't need to check that db streams have same min/max length as we should check that at db creation
        return new KmerStream<>(
                StreamUtils.mergeSortedStreams(files.stream().map(f -> f.allKmers().stream()).collect(Collectors.toList()),
                        (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
                minLength,
                maxLength,
                true);
    }

    private <S> List<ClosestInfo<S,D>> processNearestCommonKey(List<KmerWithData<S>> kmers, int maxDiff, boolean just)
    {
        List<ClosestInfo<S,D>> currentBest = new ArrayList<>(kmers.size());

        for (KmerWithData<S> k: kmers)
        {
            currentBest.add(new ClosestInfo<S,D>(k,new HashMap<>(), maxDiff));
        }

        byte[] keybytes = new byte[keyLength];
        System.arraycopy(kmers.get(0).getKmer().getRawBytes(),0,keybytes,0,keyLength);
        TreeSet<Integer> closekeys = KmerUtils.getCloseKeys(keybytes, maxDiff);

        for (int key: closekeys)
        {
            Root<D> r = new Root<>(maxLength,minLength,dataType);
            for (KmerFile<D> f: files)
            {
                //f.kmers(key).stream().forEach(k -> r.addKmer(k));
                r.addKmers(f.kmers(key));
            }
            for (int i = 0; i < kmers.size(); i++)
            {
                KmerWithData<S> k = kmers.get(i);
                ClosestInfo<S,D> newci = r.closestKmers(k, maxDiff, just);
                ClosestInfo<S,D> oldci = currentBest.get(i);
                if ((newci.getMinDist() == oldci.getMinDist()) || !just)
                {
                    oldci.merge(newci);
                }
                if ((newci.getMinDist() < oldci.getMinDist()) && just)
                {
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

    private MergeableDataType<D> dataType;
    private int keyLength;
    private int minLength;
    private int maxLength;
    private List<KmerFile<D>> files;
}
