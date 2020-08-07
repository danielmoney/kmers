package Database;

import Files.IndexedOutputFile;
import Files.StandardIndexedOutputFile;
import Files.ZippedIndexedOutputFile;
import KmerFiles.ReadDBFileCreator;
import KmerFiles.RefDBFileCreator;
import OtherFiles.KmersFromFile;
import Reads.ReadIDMapping;
import Reads.ReadPos;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class MakeDatabase
{
    public static void main(String[] args) throws Exception
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());

        options.addOption(Option.builder("k").hasArg().desc("Max kmer length").build());
        options.addOption(Option.builder("j").hasArg().desc("Min kmer length").build());

        options.addOption(Option.builder("l").hasArg().desc("Key length").build());

        options.addOption(Option.builder("c").hasArg().desc("Cache size").build());

        OptionGroup dbtype = new OptionGroup();
        dbtype.addOption(Option.builder("r").desc("Create reference database").build());
        dbtype.addOption(Option.builder("d").desc("Create reads database").build());
        dbtype.isRequired();
        options.addOptionGroup(dbtype);

        options.addOption(Option.builder("m").hasArg().desc("Write read map to file").build());

        options.addOption(Option.builder("h").desc("Human readable output").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        List<String> a = commands.getArgList();
        int k = Integer.parseInt(commands.getOptionValue('k',"32"));
        int j = Integer.parseInt(commands.getOptionValue('j',"24"));
        int l = Integer.parseInt(commands.getOptionValue('l',"6"));
        int z = Integer.parseInt(commands.getOptionValue('z',"5"));
        int c = Integer.parseInt(commands.getOptionValue('c',"1000"));

        IndexedOutputFile<Integer> out;
        if (commands.hasOption('z'))
        {
            out = new ZippedIndexedOutputFile<>(new File(a.get(1)), new File(a.get(2)), z);
        }
        else
        {
            out = new StandardIndexedOutputFile<>(new File(a.get(1)), new File(a.get(2)));
        }

        if (commands.hasOption('r'))
        {
            RefDBFileCreator dbc = new RefDBFileCreator(new File(a.get(1) + ".tmp"),new File(a.get(2) + ".tmp"),l,k,c);

            KmersFromFile<Integer> kf = KmersFromFile.getFQtoRefDBInstance(k, j);

            BufferedReader in = ZipOrNot.getBufferedReader(new File(a.get(0)));
            kf.streamFromFile(in).forEach(dbc);

            dbc.create(out, !commands.hasOption('h'));

            dbc.close();
        }
        if (commands.hasOption('d'))
        {
            ReadDBFileCreator dbc = new ReadDBFileCreator(new File(a.get(1) + ".tmp"),new File(a.get(2) + ".tmp"),l,k,c);

            PrintWriter outReadMap = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('m')))))));

            ReadIDMapping map = new ReadIDMapping(outReadMap);

            KmersFromFile<ReadPos> kf = KmersFromFile.getFAtoReadDBInstance(k, j, map);

            BufferedReader in = ZipOrNot.getBufferedReader(new File(a.get(0)));
            kf.streamFromFile(in).forEach(dbc);

            dbc.create(out, !commands.hasOption('h'));

            dbc.close();

            outReadMap.close();
        }

        System.out.println(sdf.format(new Date()));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}