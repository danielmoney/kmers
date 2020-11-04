package Utils;

import OtherFiles.KmersFromFile;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.Buffer;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ShortKmerCount
{
    public static void main(String[] args) throws IOException, ParseException
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").hasArg().required().desc("Input file").build());
        options.addOption(Option.builder("o").hasArg().required().desc("Output file").build());
        options.addOption(Option.builder("k").hasArg().required().desc("Kmer size").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(
                new FileOutputStream(new File(commands.getOptionValue('o'))))));

        BufferedReader br = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('i')));
        int k = Integer.parseInt(commands.getOptionValue('k'));
        int numPoss = 1;
        for (int i = 0; i < k; i++)
        {
            numPoss *= 4;
        }

        int[] counts = new int[numPoss];

        //KmersFromFile<Void> kf = KmersFromFile.getFQtoCountInstance(32);
        KmersFromFile<Void> kf = KmersFromFile.getFQtoCountInstance(k);

        kf.streamFromFile(br).stream().mapToInt(kmer -> kmer.getKmer().key(k)).forEach(i -> counts[i] = counts[i] + 1);

        br.close();

        for (int i = 0; i < numPoss; i++)
        {
            out.println(counts[i]);
        }

        out.close();

        System.out.println(sdf.format(new Date()));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}