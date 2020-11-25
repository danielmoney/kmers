package Utils;

import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.ListOrderedLatches;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import DataTypes.IntDataType;
import DataTypes.MapDataType;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.IndexedOutputFile;
import IndexedFiles.IndexedOutputFileSet;
import Reads.ReadPos;
import Reads.ReadPosDataType;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class ReadClassifier
{
    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        /*
        -i  Input file
        -o  Output file
        -t  Threads
         */
        System.out.println(sdf.format(new Date()));

        Options options = new Options();
        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt = new DataPairDataType<>(new ReadPosDataType(),
                new MapDataType<>(new IntDataType(), new CountDataType("x","|")), "\t");

        IndexedInputFile<Integer> in = new IndexedInputFile<>(new File(commands.getOptionValue('i')), new IntCompressor());

        IndexedOutputFileSet<Integer> out = new IndexedOutputFileSet<>(f -> new IndexedOutputFile<>(f,new IntCompressor(),true,5),
                new File(commands.getOptionValue('o')));
        ListOrderedLatches<Integer> latches = new ListOrderedLatches<>(new ArrayList<>(in.indexes()));


        if (commands.hasOption('t'))
        {
            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
        }

        LimitedQueueExecutor<Void> ex = new LimitedQueueExecutor<>();

        for (Integer index: in.indexes())
        {
            ex.submit(new ProcessIndex(in, index, idt, out, latches));
        }

        ex.shutdown();

        out.close();
        in.close();

        System.out.println(sdf.format(new Date()));
    }

    private static class ProcessIndex implements Callable<Void>
    {
        private ProcessIndex(IndexedInputFile<Integer> in, int index,
                             DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt,
                             IndexedOutputFileSet<Integer> out,
                             ListOrderedLatches<Integer> latches)
        {
            this.in = in;
            this.index = index;
            this.idt = idt;
            this.out = out;
            this.latches = latches;
        }

        public Void call()
        {
            BufferedReader br;
            try
            {
                br = new BufferedReader(new InputStreamReader(in.getInputStream(index)));
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
            }
            Iterator<String> inData = br.lines().iterator();

            DataPair<ReadPos, Map<Integer, TreeCountMap<Integer>>> data;
            int curRead = -1;
            List<Set<Integer>> taxids = new LinkedList<>();
            List<DataPair<Integer,Integer>> called = new LinkedList<>();
            if (inData.hasNext())
            {
                data = idt.fromString(inData.next());
                curRead = data.getA().getRead();
                if (data.getB().containsKey(0))
                {
                    taxids.add(data.getB().get(0).keySet());
                }
            }

            while (inData.hasNext())
            {
                 data = idt.fromString(inData.next());
                 if (data.getA().getRead() != curRead)
                 {
                     Integer call = processRead(taxids);
                     if (call != null)
                     {
                         called.add(new DataPair<>(curRead,call));
                     }
                     curRead = data.getA().getRead();
                     taxids = new LinkedList<>();
                 }
                 if (data.getB().containsKey(0))
                 {
                     taxids.add(data.getB().get(0).keySet());
                 }
            }

            Integer call = processRead(taxids);
            if (call != null)
            {
                called.add(new DataPair<>(curRead,call));
            }

            byte[][] temp = new byte[called.size()][];
            int c = 0;
            int s = 0;
            DataPairDataType<Integer,Integer> odt = new DataPairDataType<>(new IntDataType(), new IntDataType(), "\t");
            for (DataPair<Integer,Integer> cc: called)
            {
                temp[c] = (odt.toString(cc) + "\n").getBytes();
                s += temp[c].length;
                c++;
            }
            ByteBuffer bb = ByteBuffer.allocate(s);
            for (byte[] b: temp)
            {
                bb.put(b);
            }

            try
            {
                latches.hold(index);
                out.setCurrentKey(index);
                out.write(bb.array());
                latches.done();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            return null;
        }

        public Integer processRead(List<Set<Integer>> taxids)
        {
            int call = -1;

            for (Set<Integer> tids : taxids)
            {
                if (tids.size() == 1)
                {
                    int cur = tids.iterator().next();;
                    if (call == -1)
                    {
                        call = cur;
                    }
                    else
                    {
                        if (cur != call)
                        {
                            call = -2;
                        }
                    }
                }
            }

            if (call >= 0)
            {
                for (Set<Integer> tids: taxids)
                {
                    if (!tids.contains(call))
                    {
                        call = -2;
                    }
                }
            }

            if (call >= 0)
            {
                return call;
            }
            else
            {
                return null;
            }
        }

        private IndexedInputFile<Integer> in;
        private int index;
        private DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt;
        private IndexedOutputFileSet<Integer> out;
        private ListOrderedLatches<Integer> latches;
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
