package Utils;

import Compression.IntCompressor;
import Compression.StringCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.ListOrderedIndexedOutput;
import Concurrent.ListOrderedLatches;
import CountMaps.CountMap;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.*;
import IndexedFiles2.ComparableIndexedOutputFileCache2;
import IndexedFiles2.IndexedInputFile2;
import IndexedFiles2.IndexedOutputFile2;
import IndexedFiles2.IndexedOutputFileSet2;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Reads.ReadPos;
import Reads.ReadPosDataType;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class CollectByRead
{
    public static void main(String[] args) throws Exception
    {
        /*
        -i  Input file
        -o  Output gile
         */
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> rdt = ResultsDataType.getReadReferenceInstance();
        ResultsFile<Set<ReadPos>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(commands.getOptionValue('i')), rdt);

        File tmpFile = new File(commands.getOptionValue('i') + ".tmp");
        IndexedOutputFileSet2<Integer> o = new IndexedOutputFileSet2<>(f -> new IndexedOutputFile2<>(f,new IntCompressor(), true, 5), tmpFile);
        ComparableIndexedOutputFileCache2<Integer> cache = new ComparableIndexedOutputFileCache2<>(1000, o);


        DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt = new DataPairDataType<>(new ReadPosDataType(),
                new MapDataType<>(new IntDataType(), new CountDataType("x","|")), "\t");

        in.stream().forEach(kwd -> processKmer(kwd, cache, odt));
        cache.close();

        IndexedInputFile2<Integer> in2 = new IndexedInputFile2<>(tmpFile, new IntCompressor());

        IndexedOutputFileSet2<Integer> out = new IndexedOutputFileSet2<>(f -> new IndexedOutputFile2<>(f,new IntCompressor(),true,5),
                new File(commands.getOptionValue('o')));
        ListOrderedLatches<Integer> latches = new ListOrderedLatches<>(new ArrayList<>(in2.indexes()));

        LimitedQueueExecutor<Void> ex = new LimitedQueueExecutor<>();

        for (Integer index: in2.indexes())
        {
            ex.submit(new ProcessIndex(in2, index, odt, out, latches));
        }

        ex.shutdown();

        out.close();
        in2.close();

        for (File f: o.getCreated())
        {
            f.delete();
        }

        System.out.println(sdf.format(new Date()));
    }

    private static class ProcessIndex implements Callable<Void>
    {
        private ProcessIndex(IndexedInputFile2<Integer> in, int index,
                             DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt,
                             //ListOrderedIndexedOutput<Integer> out)
                             IndexedOutputFileSet2<Integer> out,
                             ListOrderedLatches<Integer> latches)
        {
            this.in = in;
            this.index = index;
            this.odt = odt;
            this.out = out;
            this.latches = latches;
        }

        public Void call()
        {
            TreeSet<DataPair<ReadPos,Map<Integer,TreeCountMap<Integer>>>> set = new TreeSet<>((dp1,dp2) -> dp1.getA().compareTo(dp2.getA()));
            try
            {
                DataInputStream is = new DataInputStream(in.getInputStream(index));

                try
                {
                    while (true)
                    {
                        set.add(odt.decompress(is));
                    }
                }
                catch (EOFException ex)
                {
                    // Don't need to do anything here
                }
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
            }

            byte[] data = set.stream().map(dp -> odt.toString(dp)).collect(Collectors.joining("\n")).getBytes();

            try
            {
                latches.hold(index);
                out.setCurrentKey(index);
                out.write(data);
                latches.done();
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

        private IndexedInputFile2<Integer> in;
        private int index;
        private DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt;
        private IndexedOutputFileSet2<Integer> out;
        private ListOrderedLatches<Integer> latches;
    }

    private static void processKmer(KmerWithData<DataPair<Set<ReadPos>, Set<DataPair<KmerDiff, TreeCountMap<Integer>>>>> kwd,
                                    ComparableIndexedOutputFileCache2<Integer> cache,
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
                cache.add((newData.getA().getRead() / 1000) * 1000, odt.compress(newData));
            }
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
