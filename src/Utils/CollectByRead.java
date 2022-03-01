package Utils;

import Compression.IntCompressor;
import Compression.StringCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.ListOrderedLatches;
import CountMaps.CountMap;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.*;
import IndexedFiles.ComparableIndexedOutputFileCache;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.IndexedOutputFile;
import IndexedFiles.IndexedOutputFileSet;
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

        options.addOption(Option.builder("f").hasArg().desc("Temporary files location").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> rdt = ResultsDataType.getReadReferenceInstance();
        ResultsFile<Set<ReadPos>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(commands.getOptionValue('i')), rdt);

        String tempLoc = commands.getOptionValue('f', "");
        File tmpFile = new File(tempLoc + "temp.tmp");
        IndexedOutputFileSet<Integer> o = new IndexedOutputFileSet<>(f -> new IndexedOutputFile<>(f,new IntCompressor(), true, 5), tmpFile);
        ComparableIndexedOutputFileCache<Integer> cache = new ComparableIndexedOutputFileCache<>(1000, o);


        DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt = new DataPairDataType<>(new ReadPosDataType(),
                new MapDataType<>(new IntDataType(), new CountDataType("x","|")), "\t");

        in.stream().forEach(kwd -> processKmer(kwd, cache, odt));
        cache.close();

        IndexedInputFile<Integer> in2 = new IndexedInputFile<>(tmpFile, new IntCompressor());

        IndexedOutputFileSet<Integer> out = new IndexedOutputFileSet<>(f -> new IndexedOutputFile<>(f,new IntCompressor(),true,5),
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
        private ProcessIndex(IndexedInputFile<Integer> in, int index,
                             DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt,
                             //ListOrderedIndexedOutput<Integer> out)
                             IndexedOutputFileSet<Integer> out,
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

        private IndexedInputFile<Integer> in;
        private int index;
        private DataPairDataType<ReadPos,Map<Integer,TreeCountMap<Integer>>> odt;
        private IndexedOutputFileSet<Integer> out;
        private ListOrderedLatches<Integer> latches;
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
