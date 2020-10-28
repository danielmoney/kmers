package Utils;

import CountMaps.TreeCountMap;
import DataTypes.*;
import Exceptions.UnknownTaxaException;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Kmers.KmerWithDataDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;
import Taxonomy.Taxa;
import Taxonomy.Tree;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class MatchLimiter
{
    public static void main(String[] args) throws IOException, ParseException
    {
        /*
        -i  Input file
        -o  Output File
        -E  Excluded taxa
        -x  Taxonomy
         */
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("E").required().hasArg().desc("Excluded taxa file").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());
        options.addOption(Option.builder("x").hasArg().desc("Taxonomy file").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> dt = ResultsDataType.getReadReferenceInstance();
        ResultsFile<Set<ReadPos>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(commands.getOptionValue('i')), dt);

        PrintWriter out = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                new FileOutputStream(new File(commands.getOptionValue('o')))), 5)));


//        Tree t = null;

        TreeSet<Integer> limitTaxa = new TreeSet<>();
//        limitTaxa.add(4113);
//        limitTaxa.add(3193);
        BufferedReader efile = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('E')));
        efile.lines().map(l -> Integer.parseInt(l)).forEach(i -> limitTaxa.add(i));
        efile.close();


        if (!commands.hasOption('x'))
        {
            in.stream().map(kwd -> limit(kwd, limitTaxa))
                    .filter(kwd -> !kwd.getData().getB().isEmpty()) // Dom't output kmers that now have no matches
                    .forEach(kwd -> out.println(dt.toString(kwd)));
        }
        else
        {
            Tree t = Tree.getInstanceFromFile(new File(commands.getOptionValue('x')));
            in.stream().map(kwd -> limitBelow(kwd,limitTaxa,t))
                    .filter(kwd -> !kwd.getData().getB().isEmpty()) // Dom't output kmers that now have no matches
                    .forEach(kwd -> out.println(dt.toString(kwd)));
        }

        in.close();
        out.close();

        System.out.println(sdf.format(new Date()));
    }

    public static <S> KmerWithData<DataPair<S,Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> limit(
            KmerWithData<DataPair<S,Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> input,
            Set<Integer> limitTaxa)
    {
        Iterator<DataPair<KmerDiff,TreeCountMap<Integer>>> it = input.getData().getB().iterator();

        while (it.hasNext())
        {
            DataPair<KmerDiff,TreeCountMap<Integer>> dp = it.next();
            for (Integer t: limitTaxa)
            {
                dp.getB().remove(t);
            }
            if (dp.getB().isEmpty())
            {
                it.remove(); // Remove KmerDiff if there are no matches for it
            }
        }

//        for (DataPair<KmerDiff,TreeCountMap<Integer>> r: input.getData().getB())
//        {
//            for (Integer t: limitTaxa)
//            {
//                r.getB().remove(t);
//            }
//        }

        return input;
    }

    public static <S> KmerWithData<DataPair<S,Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> limitBelow(
            KmerWithData<DataPair<S,Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> input,
            Set<Integer> limitTaxa, Tree tax)
    {
        Iterator<DataPair<KmerDiff,TreeCountMap<Integer>>> it = input.getData().getB().iterator();

        while (it.hasNext())
        {
            DataPair<KmerDiff,TreeCountMap<Integer>> dp = it.next();

            Iterator<Map.Entry<Integer,Long>> it2 = dp.getB().entrySet().iterator();
            while (it2.hasNext())
            //for (int t: dp.getB().keySet())
            {
                boolean remove = false;
//                int c = t;
                int c = it2.next().getKey();

                while (!remove && c!= -1)
                {
                    if (limitTaxa.contains(c))
                    {
                        remove = true;
                    }
                    try
                    {
                        c = tax.getNode(c).getParentID();
                    }
                    catch (UnknownTaxaException e)
                    {
                        c = -1; // Shouldn't get here but in case we do
                    }
                }

                if (remove)
                {
//                    dp.getB().remove(t);
                    it2.remove();
                }
            }

            if (dp.getB().isEmpty())
            {
                it.remove(); // Remove KmerDiff if there are no matches for it
            }
        }

        return input;
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
