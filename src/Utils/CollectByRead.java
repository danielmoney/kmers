package Utils;

import Compression.IntCompressor;
import Compression.StringCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.ListOrderedIndexedOutput;
import CountMaps.CountMap;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.*;
import IndexedFiles.ComparableIndexedOutputFileCache;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.ZippedIndexedInputFile;
import IndexedFiles.ZippedIndexedOutputFile;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Reads.ReadPos;
import Reads.ReadPosDataType;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class CollectByRead
{
    public static void main(String[] args) throws Exception
    {
        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> rdt = ResultsDataType.getReadReferenceInstance();
        ResultsFile<Set<ReadPos>, TreeCountMap<Integer>> in = new ResultsFile<>(new File("test.gz"), rdt);

        File tmpFile = new File("reads.gz.tmp");
        ComparableIndexedOutputFileCache<Integer> cache = new ComparableIndexedOutputFileCache<>(1000,
                new ZippedIndexedOutputFile<>(tmpFile,new IntCompressor(),true,5));

        DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt = new DataPairDataType<>(new ReadPosDataType(),
                new MapDataType<>(new IntDataType(), new CountDataType("x","|")), "\t");

        in.stream().forEach(kwd -> processKmer(kwd, cache,odt));
        cache.close();

        IndexedInputFile<Integer> in2 = new ZippedIndexedInputFile<>(tmpFile, new IntCompressor());
        ListOrderedIndexedOutput<Integer> out = new ListOrderedIndexedOutput<>(
                new ZippedIndexedOutputFile<>(new File("reads.gz"), new IntCompressor(),true,5),
                in2.indexes());

        LimitedQueueExecutor<Void> ex = new LimitedQueueExecutor<>();

        for (Integer index: in2.indexes())
        {
            ex.submit(new ProcessIndex(in2, index, odt, out));
        }

        ex.shutdown();

        out.close();
        in2.close();

        tmpFile.delete();
    }

    private static class ProcessIndex implements Callable<Void>
    {
        private ProcessIndex(IndexedInputFile<Integer> in, int index,
                             DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt,
                             ListOrderedIndexedOutput<Integer> out)
        {
            this.in = in;
            this.index = index;
            this.odt = odt;
            this.out = out;
        }

        public Void call()
        {
            ByteBuffer bb = ByteBuffer.wrap(in.data(index));
            TreeSet<DataPair<ReadPos,Map<Integer,TreeCountMap<Integer>>>> set = new TreeSet<>((dp1,dp2) -> dp1.getA().compareTo(dp2.getA()));
            while (bb.hasRemaining())
            {
                set.add(odt.decompress(bb));
            }

            byte[] data = set.stream().map(dp -> odt.toString(dp)).collect(Collectors.joining("\n")).getBytes();

            try
            {
                out.write(data, index);
            }
            catch (InterruptedException e)
            {
                // In the normal course of things we shouldn't get here so something unusal has happened
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            return null;
        }

        private IndexedInputFile<Integer> in;
        private int index;
        private DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt;
        private ListOrderedIndexedOutput<Integer> out;
    }

    private static void processKmer(KmerWithData<DataPair<Set<ReadPos>, Set<DataPair<KmerDiff, TreeCountMap<Integer>>>>> kwd,
                                    ComparableIndexedOutputFileCache<Integer> cache,
                                    DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt)
    {
        try
        {
            Map<Integer, TreeCountMap<Integer>> distTaxa = new TreeMap<>();
            for (DataPair<KmerDiff, TreeCountMap<Integer>> dp : kwd.getData().getB())
            {
                if (!distTaxa.containsKey(dp.getA().dist()))
                {
                    distTaxa.put(dp.getA().dist(), new TreeCountMap<>());
                }
                distTaxa.get(dp.getA().dist()).addAll(dp.getB());
            }
            for (ReadPos rp : kwd.getData().getA())
            {
                DataPair<ReadPos, Map<Integer, TreeCountMap<Integer>>> newData = new DataPair<>(rp, distTaxa);
                //            System.out.println(odt.toString(newData));
//                cache.add(newData.getA().getRead() / 1000, odt.toString(newData) + "\n");
//                cache.add(newData.getA().getRead() / 1000, odt.compress(newData));
                cache.add((newData.getA().getRead() / 1000) * 1000, odt.compress(newData));
            }
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }
}
