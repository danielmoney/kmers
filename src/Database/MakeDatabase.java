package Database;

import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import CountMaps.TreeCountMap;
import DataTypes.DataCollector;
import IndexedFiles.IndexedOutputFile;
import IndexedFiles.StandardIndexedOutputFile;
import IndexedFiles.ZippedIndexedOutputFile;
import KmerFiles.FileCreator;
import Kmers.Dust;
import Kmers.KmerStream;
import Kmers.RunOfSame;
import OtherFiles.KmersFromFile;
import OtherFiles.ReadIDMapping;
import Reads.ReadPos;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
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
        dbtype.isRequired();
        options.addOptionGroup(dbtype);

        options.addOption(Option.builder("m").hasArg().desc("Write read map to file").build());

        options.addOption(Option.builder("h").desc("Human readable output").build());

        options.addOption(Option.builder("u").hasArg().desc("Filter kmers with of dust score greater than the given value").build());
        options.addOption(Option.builder("r").hasArg().desc("Filter kmers with runs of the same base longer than the given value").build());

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

            KmersFromFile<Integer> kf = KmersFromFile.getFAtoRefDBInstance(j, k);

            filterAndCreate(kf, dbc, commands);
        }
        if (commands.hasOption('q'))
        {
            FileCreator<ReadPos, Set<ReadPos>> dbc = new FileCreator<>(new File(commands.getOptionValue('o') + ".tmp"),l,k,c, DataCollector.getReadPosInstance(), false);

            PrintWriter outReadMap = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('m')))))));

            ReadIDMapping map = new ReadIDMapping(outReadMap);

            KmersFromFile<ReadPos> kf = KmersFromFile.getFQtoReadDBInstance(j, k, map);

            filterAndCreate(kf, dbc, commands);
        }

        System.out.println(sdf.format(new Date()));
    }

    private static <D> void filterAndCreate(KmersFromFile<D> kf, FileCreator<D,?> dbc, CommandLine commands) throws Exception
    {
        BufferedReader in = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('i')));

        KmerStream<D> kstream = kf.streamFromFile(in);

        if (commands.hasOption('u'))
        {
            kstream = kstream.filter(new Dust(Integer.parseInt(commands.getOptionValue('d'))));
        }
        if (commands.hasOption('r'))
        {
            kstream = kstream.filter(new RunOfSame(Integer.parseInt(commands.getOptionValue('r'))));
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

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}