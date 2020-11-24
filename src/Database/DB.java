package Database;

import Concurrent.LimitedQueueExecutor;
import DataTypes.DataPair;
import DataTypes.MergeableDataType;
import Exceptions.InconsistentDataException;
import KmerFiles.KmerFile;
import Kmers.KmerDiff;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Streams.StreamUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DB<D>
{
    public DB(KmerFile<D> file) throws InconsistentDataException
    {
        this(Collections.singletonList(file));
    }

    public DB(List<KmerFile<D>> files) throws InconsistentDataException
    {
        // Stop this being modified after creation by creating a new copy as we do some checks on the files at construction
        this.files = new LinkedList<>(files);

        this.dataType = files.get(0).getDataType();
        for (KmerFile<D> f: files)
        {
            if (!Arrays.equals(f.getDataType().getID(), this.dataType.getID()))
            {
                throw new InconsistentDataException("Files contain different datatypes");
            }
        }

        if (files.isEmpty())
        {
            throw new IllegalArgumentException("Empty file list");
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
                throw new InconsistentDataException("Files contains different kmer parameters (min/max length, key length or reverse complement included");
            }
        }

        maxKey = 1;
        for (int i = 0; i< keyLength; i++)
        {
            maxKey *=4;
        }
    }

    public <S> KmerStream<DataPair<S, Set<DataPair<KmerDiff,D>>>> getNearestKmers(KmerStream<S> searchKmers, int maxDiff, boolean just) throws InconsistentDataException
    {
        if ((searchKmers.getMinLength() < minLength) || (searchKmers.getMaxLength() > maxLength))
        {
            throw new InconsistentDataException("Search kmers contain kmers of a length inconsistent with the database");
        }

        boolean quick = ((searchKmers.getMinLength() == searchKmers.getMaxLength()) && (maxDiff == 0)); //Quick match

        Stream<List<KmerWithData<S>>> groupedStream = StreamUtils.groupedStream(searchKmers.stream(), (kwd1, kwd2) -> kwd1.getKmer().key(keyLength) == kwd2.getKmer().key(keyLength), Collectors.toList());
        ProcessCommonSpliterator<S> spliterator = new ProcessCommonSpliterator<>(groupedStream, maxDiff, just, quick, searchKmers.getMinLength(), searchKmers.getMaxLength());
        return new KmerStream<>(
                StreamUtils.concetenateStreams(StreamSupport.stream(spliterator, false).onClose(() -> spliterator.close()).map(l -> l.stream())).map(kwd -> mapToResult(kwd)),
                searchKmers.getMinLength(), searchKmers.getMaxLength(), searchKmers.getRC());
    }

    private <S> KmerWithData<DataPair<S,Set<DataPair<KmerDiff,D>>>> mapToResult(KmerWithData<DataPair<S, ClosestInfoCollector<D>>> in)
    {
        Set<DataPair<KmerDiff,D>> diffs = in.getData().getB().getResult(in.getKmer());

        return new KmerWithData<>(in.getKmer(), new DataPair<>(in.getData().getA(), diffs));
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

    public int getMaxKey()
    {
        return maxKey;
    }

    private <S> List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>> quickMatchCommonKey(List<KmerWithData<S>> kmers, int maxDiff)
    {
        int key = kmers.get(0).getKmer().key(keyLength);
        int length = kmers.get(0).getKmer().length();

        return StreamUtils.matchTwoStreams(kmers.stream(),KmerUtils.restrictedStream(kmers(key),length,length, dataType).stream(),
                (kwd1, kwd2) -> new KmerWithData<>(kwd1.getKmer(), new DataPair<>(kwd1.getData(), new ClosestInfoCollector<>(kwd2))),
                (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())).collect(Collectors.toList());
    }

    private <S> List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>> processNearestCommonKey(List<KmerWithData<S>> kmers, int maxDiff, boolean just, int minK, int maxK)
    {
        List<ClosestInfoCollector<D>> currentBest = new ArrayList<>(kmers.size());

        for (KmerWithData<S> k: kmers)
        {
            currentBest.add(new ClosestInfoCollector<D>());
        }

        byte[] keybytes = new byte[keyLength];
        System.arraycopy(kmers.get(0).getKmer().getRawBytes(),0,keybytes,0,keyLength);
        TreeSet<Integer> closekeys = KmerUtils.getCloseKeys(keybytes, maxDiff);

        for (int key: closekeys)
        {
            Root<D> r = new Root<>(maxK,minK,dataType);
            for (KmerFile<D> f: files)
            {
                r.addKmers(f.kmers(key));
            }
            for (int i = 0; i < kmers.size(); i++)
            {
                KmerWithData<S> k = kmers.get(i);
                ClosestInfoCollector<D> newci = r.closestKmers(k, maxDiff, just);
                ClosestInfoCollector<D> oldci = currentBest.get(i);
                if ((newci.getMinDist() == oldci.getMinDist()) || !just)
                {
                    oldci.addAll(newci);
                }
                if ((newci.getMinDist() < oldci.getMinDist()) && just)
                {
                    currentBest.set(i,newci);
                }
            }
        }

        List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>> ret = new ArrayList<>(kmers.size());

        for (int i = 0; i < kmers.size(); i++)
        {
            ret.add(new KmerWithData<>(kmers.get(i).getKmer(),
                    new DataPair<>(kmers.get(i).getData(), currentBest.get(i))));
        }

        return ret;
    }

    private class ProcessCommonSpliterator<S> implements Spliterator<List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>>>
    {
        private ProcessCommonSpliterator(Stream<List<KmerWithData<S>>> inputStream, int maxDiff, boolean just, boolean quick, int minK, int maxK)
        {
            this.input = inputStream.iterator();
            ex = Executors.newFixedThreadPool(threads);
            futures = new LinkedList<>();
            for (int i = 0; i < threads; i++)
            {
                if (input.hasNext())
                {
                    List<KmerWithData<S>> l = input.next();
//                    System.out.println("*"+l.get(0).getKmer());
                    if (quick)
                    {
                        futures.add(ex.submit(() -> quickMatchCommonKey(l, maxDiff)));
                    }
                    else
                    {
                        futures.add(ex.submit(() -> processNearestCommonKey(l, maxDiff, just, minK, maxK)));
                    }
                }
            }
            this.maxDiff = maxDiff;
            this.just = just;
            this.quick = quick;
            this.minK = minK;
            this.maxK = maxK;
        }

        public boolean tryAdvance(Consumer<? super List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>>> consumer)
        {
            if (!futures.isEmpty())
            {
                try
                {
                    Future<List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>>> future = futures.poll();
//                    System.out.println("Done another");
                    if (input.hasNext())
                    {
                        List<KmerWithData<S>> l = input.next();
                        if (quick)
                        {
                            futures.add(ex.submit(() -> quickMatchCommonKey(l, maxDiff)));
                        }
                        else
                        {
                            futures.add(ex.submit(() -> processNearestCommonKey(l, maxDiff, just, minK, maxK)));
                        }
                    }
                    else
                    {
                        ex.shutdown();
                    }
                    consumer.accept(future.get());
                }
                catch (InterruptedException e)
                {
                    // Shouldn't get here in the normal course of things so....
                    System.out.println(e);
                    throw new RuntimeException(e);
                }
                catch (ExecutionException e)
                {
                    // or here...
                    System.out.println(e);
                    throw new RuntimeException(e);
                }
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>>> trySplit()
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

        public void close()
        {
            try
            {
                ex.shutdown();
                ex.awaitTermination(2400, TimeUnit.DAYS);
            }
            catch (InterruptedException e)
            {
                // Nothing much we can do here
            }
        }

        private LinkedList<Future<List<KmerWithData<DataPair<S, ClosestInfoCollector<D>>>>>> futures;
        private Iterator<List<KmerWithData<S>>> input;
        private ExecutorService ex;
        private int maxDiff;
        private boolean just;
        private boolean quick;

        private int minK;
        private int maxK;
    }

    private MergeableDataType<D> dataType;
    private int keyLength;
    private int minLength;
    private int maxLength;
    private List<KmerFile<D>> files;
    private int maxKey;

    public void setThreads(int threads)
    {
        this.threads = threads;
    }

    private int threads = Runtime.getRuntime().availableProcessors() - 1;
}
