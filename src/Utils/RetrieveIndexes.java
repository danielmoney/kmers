package Utils;

import Compression.Compressor;
import Compression.IntCompressor;
import Compression.StringCompressor;
import IndexedFiles2.IndexedInputFile2;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

public class RetrieveIndexes
{
    public static void main(String[] args) throws IOException, ParseException
    {
        // Input is file name and type (only allow pre and kmer) then this should be fairly simple I think

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input (reads) file").build());
        options.addOption(Option.builder("p").desc("Input is in preprocessed format").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        if (commands.hasOption('p'))
        {
            findAndPrint(new File(commands.getOptionValue('i')), new StringCompressor(),null);
        }
        else
        {
            findAndPrint(new File(commands.getOptionValue('i')), new IntCompressor(),-1);
        }

    }

    private static <D extends Comparable<D>> void findAndPrint(File f, Compressor<D> compressor, D ignore) throws IOException
    {
        IndexedInputFile2<D> iif;
        iif = new IndexedInputFile2<>(f, compressor);


        TreeSet<D> indexes = new TreeSet<>(iif.indexes());
        if (ignore != null)
        {
            indexes.remove(ignore);
        }
        System.out.println("First: " + indexes.first());
        System.out.println("Last : " + indexes.last());
        iif.close();
    }
}