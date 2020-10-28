package Utils;

import CountMaps.TreeCountMap;
import CountMaps.TwoKeyTreeCountMap;
import DataTypes.DataPair;
import DataTypes.ResultsDataType;
import IndexedFiles.IndexedInputFile;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Reads.ReadPos;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public class CloserCounts
{
    public static void main(String[] args) throws IOException, ParseException
    {
        /*
        -i  Input file
        -s  For two taxa id
        -o  Output file - currently prints to screen
         */

//        ResultsDataType<TreeCountMap<Integer>, TreeCountMap<Integer>> rdt = ResultsDataType.getReferenceReferenceInstance();
//        ResultsFile<TreeCountMap<Integer>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(args[0]), rdt);
        System.out.println(sdf.format(new Date()));

        Options options = new Options();
        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("s").required().hasArg().desc("Search taxa file").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(
                new FileOutputStream(new File(commands.getOptionValue('o'))))));


//        ResultsDataType<?, TreeCountMap<Integer>> rdt;
//        if ((args.length == 4) && (args[3].equals("ref")))
//        {
//            rdt = ResultsDataType.getReferenceReferenceInstance();
//        }
//        else
//        {
//            rdt = ResultsDataType.getReadReferenceInstance();
//        }
        File inf = new File(commands.getOptionValue('i'));
        ResultsDataType<?, TreeCountMap<Integer>> rdt = getResultsDataType(inf);
//        ResultsFile<?, TreeCountMap<Integer>> in = new ResultsFile<>(new File(commands.getOptionValue('i')), rdt);
        ResultsFile<?, TreeCountMap<Integer>> in = new ResultsFile<>(inf, rdt);

        BufferedReader sfile = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('s')));
        Integer taxA = Integer.valueOf(sfile.readLine());
        Integer taxB = Integer.valueOf(sfile.readLine());

        TwoKeyTreeCountMap<Integer,Integer> count = new TwoKeyTreeCountMap<>();
//        Integer taxA = Integer.valueOf(args[1]);
//        Integer taxB = Integer.valueOf(args[2]);

        in.stream().forEach(kwd -> updateCount(count, kwd, taxA, taxB));

        for (Map.Entry<Integer, TreeCountMap<Integer>> e: count.entrySet())
        {
            for (Map.Entry<Integer,Long> e2: e.getValue().entrySet())
            {
                String t1 = (e.getKey()==Integer.MAX_VALUE)?"-":e.getKey().toString();
                String t2 = (e2.getKey()==Integer.MAX_VALUE)?"-":e2.getKey().toString();
                out.println(t1 + "\t" + t2 + "\t" + e2.getValue());
            }
        }

        out.close();

        System.out.println(sdf.format(new Date()));
    }

    public static ResultsDataType<?, TreeCountMap<Integer>> getResultsDataType(File f) throws IOException
    {
        BufferedReader in = ZipOrNot.getBufferedReader(f);
        String line = in.readLine();
        in.close();

        if (line.contains(":"))
        {
            return ResultsDataType.getReadReferenceInstance();
        }
        else
        {
            return ResultsDataType.getReferenceReferenceInstance();
        }
    }


    public static void updateCount(TwoKeyTreeCountMap<Integer, Integer> count,
//                                   KmerWithData<DataPair<TreeCountMap<Integer>, Set<DataPair<KmerDiff, TreeCountMap<Integer>>>>> kwd,
                                   KmerWithData<? extends DataPair<?, Set<DataPair<KmerDiff, TreeCountMap<Integer>>>>> kwd,
                                   Integer taxA, Integer taxB)
    {
        Integer minTaxA = Integer.MAX_VALUE;
        Integer minTaxB = Integer.MAX_VALUE;

        for (DataPair<KmerDiff, TreeCountMap<Integer>> c: kwd.getData().getB())
        {
            if (c.getB().containsKey(taxA))
            {
                minTaxA = Math.min(minTaxA, c.getA().dist());
            }
            if (c.getB().containsKey(taxB))
            {
                minTaxB = Math.min(minTaxB, c.getA().dist());
            }
        }

        count.add(minTaxA,minTaxB);
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
