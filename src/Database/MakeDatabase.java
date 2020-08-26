package Database;

import Compression.IntCompressor;
import Compression.StringCompressor;
import Concurrent.LimitedQueueExecutor;
import CountMaps.TreeCountMap;
import DataTypes.DataCollector;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import DataTypes.IntDataType;
import IndexedFiles.*;
import KmerFiles.FileCreator;
import Kmers.*;
import OtherFiles.KmersFromFile;
import OtherFiles.ReadIDMapping;
import Reads.ReadPos;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

public class MakeDatabase
{
    public static void main(String[] args) throws Exception
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").hasArg().required().desc("Input file").build());
        options.addOption(Option.builder("o").hasArg().required().desc("Output file").build());

        OptionGroup zipoptions = new OptionGroup();
        zipoptions.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());
        zipoptions.addOption(Option.builder("Z").hasArg().desc("Unzipped output").build());
        options.addOptionGroup(zipoptions);

        options.addOption(Option.builder("K").hasArg().desc("Max kmer length").build());
        options.addOption(Option.builder("k").hasArg().desc("Min kmer length").build());

        options.addOption(Option.builder("l").hasArg().desc("Key length").build());

        options.addOption(Option.builder("c").hasArg().desc("Cache size").build());

        OptionGroup dbtype = new OptionGroup();
        dbtype.addOption(Option.builder("a").desc("Input is in FATSA format").build());
        dbtype.addOption(Option.builder("q").desc("Input is in FASTQ format").build());
        dbtype.addOption(Option.builder("p").desc("Input is in preprocessed format").build());
        dbtype.isRequired();
        options.addOptionGroup(dbtype);

        options.addOption(Option.builder("r").hasArg().desc("Write read map to file").build());

        options.addOption(Option.builder("m").hasArg().desc("Seq id to taxa id map (for use with -a").build());

        options.addOption(Option.builder("h").desc("Human readable output").build());

        options.addOption(Option.builder("D").hasArg().desc("Filter kmers with of dust score greater than the given value").build());
        options.addOption(Option.builder("R").hasArg().desc("Filter kmers with runs of the same base longer than the given value").build());

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);
        int k = Integer.parseInt(commands.getOptionValue('K',"32"));
        int j = Integer.parseInt(commands.getOptionValue('k',"24"));
        int l = Integer.parseInt(commands.getOptionValue('l',"6"));
        int c = Integer.parseInt(commands.getOptionValue('c',"1000"));


        if (commands.hasOption("t"))
        {
            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
        }


        if (commands.hasOption('a'))
        {
            // True is to include reverse complement - should have as a optional param as well?
            FileCreator<Integer, TreeCountMap<Integer>> dbc = new FileCreator<>(new File(commands.getOptionValue('o') + ".tmp"),l,k,c, DataCollector.getCountInstance(), true);

            KmersFromFile<Integer> kf;
            if (commands.hasOption('m'))
            {
                BufferedReader br = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('m')));
                Map<String, Integer> map = new HashMap<>();
                br.lines().forEach(line -> {
                    String[] parts = line.split("\t");
                    map.put(parts[0], Integer.parseInt(parts[1]));
                });
                kf = KmersFromFile.getFAtoRefDBInstance(j, k, map);
            }
            else
            {
                kf = KmersFromFile.getFAtoRefDBInstance(j, k);
            }

            BufferedReader in = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('i')));

            filterAndCreate(kf.streamFromFile(in), dbc, commands);
        }
        if (commands.hasOption('q'))
        {
            FileCreator<ReadPos, Set<ReadPos>> dbc = new FileCreator<>(new File(commands.getOptionValue('o') + ".tmp"),l,k,c, DataCollector.getReadPosInstance(), false);

            PrintWriter outReadMap = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('r')))))));

            ReadIDMapping map = new ReadIDMapping(outReadMap);

            KmersFromFile<ReadPos> kf = KmersFromFile.getFQtoReadDBInstance(j, k, map);

            BufferedReader in = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('i')));

            filterAndCreate(kf.streamFromFile(in), dbc, commands);
        }
        if (commands.hasOption('p'))
        {
            FileCreator<Integer, TreeCountMap<Integer>> dbc = new FileCreator<>(new File(commands.getOptionValue('o') + ".tmp"),l,k,c, DataCollector.getCountInstance(), true);

            IndexedInputFile in = new ZippedIndexedInputFile(new File(commands.getOptionValue('i')), new StringCompressor());
            KmerStream<Integer> kstream = new KmerStream(StreamSupport.stream(new PreProcessedSpliterator(in, j, k), false),j,k,false);

//            kstream.limit(100).forEach(kwd -> System.out.println(kwd));
            filterAndCreate(kstream, dbc, commands);
        }

        System.out.println(sdf.format(new Date()));
    }

    private static <D> void filterAndCreate(KmerStream<D> kstream, FileCreator<D,?> dbc, CommandLine commands) throws Exception
    {
        BufferedReader in = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('i')));

        if (commands.hasOption('D'))
        {
            kstream = kstream.filter(new Dust(Integer.parseInt(commands.getOptionValue('D'))));
        }
        if (commands.hasOption('R'))
        {
            kstream = kstream.filter(new RunOfSame(Integer.parseInt(commands.getOptionValue('R'))));
        }

        dbc.addKmers(kstream);

        IndexedOutputFile<Integer> out;
        try
        {
            if (commands.hasOption('Z'))
            {
                out = new StandardIndexedOutputFile<>(new File(commands.getOptionValue('o')), new IntCompressor(), commands.hasOption('h'));
            }
            else
            {
                out = new ZippedIndexedOutputFile<>(new File(commands.getOptionValue('o')), new IntCompressor(), commands.hasOption('h'),
                        Integer.parseInt(commands.getOptionValue('z',"5")));
            }
        }
        catch (FileAlreadyExistsException ex)
        {
            dbc.close();
            throw ex;
        }

        dbc.create(out, commands.hasOption('h'));

        dbc.close();
    }

    private static class PreProcessedSpliterator implements Spliterator<KmerWithData<Integer>>
    {
        private PreProcessedSpliterator(IndexedInputFile<String> in, int minK, int maxK)
        {
            this.in = in;
            indexIterator = in.indexes().iterator();
            hr = in.isHumanReadable();
            if (hr)
            {
                lines = Collections.emptyIterator();
            }
            else
            {
                bb = ByteBuffer.allocate(0);
            }
            this.minK = minK;
            this.maxK = maxK;
        }

        public boolean tryAdvance(Consumer<? super KmerWithData<Integer>> consumer)
        {
            while ((curseq == null) || (curseq.length() < minK))
            {
                DataPair<Integer, Sequence> dp;
                if (hr)
                {
                    if (!lines.hasNext())
                    {
                        if (indexIterator.hasNext())
                        {
                            lines = in.lines(indexIterator.next()).iterator();
                        }
                        else
                        {
                            return false;
                        }
                    }
                    dp = integerPairDataType.fromString(lines.next());
                }
                else
                {
                    if (!bb.hasRemaining())
                    {
                        if (indexIterator.hasNext())
                        {
                            bb = ByteBuffer.wrap(in.data(indexIterator.next()));
                        }
                        else
                        {
                            return false;
                        }
                    }
                    dp = integerPairDataType.decompress(bb);
                }
                curid = dp.getA();
                curseq = dp.getB();
                curstart = 0;
                processed = false;
            }

            int end = Math.min(curstart+maxK,curseq.length());
            int l = end - curstart;
            byte[] kbytes = new byte[l];
            System.arraycopy(curseq.getRawBytes(),curstart,kbytes,0,l);
            curstart++;
            if ((curseq.length() - curstart) < minK)
            {
                curseq = null;
            }
            consumer.accept(new KmerWithData<>(Kmer.createUnchecked(kbytes), curid));
            return true;
        }

        @Override
        public long estimateSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public Spliterator<KmerWithData<Integer>> trySplit()
        {
            return null;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.IMMUTABLE | Spliterator.ORDERED;
        }

        private boolean processed;
        private int curstart;
        private Sequence curseq;
        private Integer curid;

        private boolean hr;

        private ByteBuffer bb;
        private Iterator<String> lines;

        private IndexedInputFile<String> in;
        private Iterator<String> indexIterator;
        private DataPairDataType<Integer,Sequence> integerPairDataType = new DataPairDataType<>(new IntDataType(), new SequenceDataType());

        private int minK;
        private int maxK;
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}