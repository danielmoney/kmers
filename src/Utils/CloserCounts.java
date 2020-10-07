package Utils;

import CountMaps.TreeCountMap;
import CountMaps.TwoKeyTreeCountMap;
import DataTypes.DataPair;
import DataTypes.ResultsDataType;
import IndexedFiles.IndexedInputFile;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Reads.ReadPos;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class CloserCounts
{
    public static void main(String[] args) throws IOException
    {
//        ResultsDataType<TreeCountMap<Integer>, TreeCountMap<Integer>> rdt = ResultsDataType.getReferenceReferenceInstance();
//        ResultsFile<TreeCountMap<Integer>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(args[0]), rdt);


        ResultsDataType<?, TreeCountMap<Integer>> rdt;
        if ((args.length == 4) && (args[3].equals("ref")))
        {
            rdt = ResultsDataType.getReferenceReferenceInstance();
        }
        else
        {
            rdt = ResultsDataType.getReadReferenceInstance();
        }
        ResultsFile<?, TreeCountMap<Integer>> in = new ResultsFile<>(new File(args[0]), rdt);

        TwoKeyTreeCountMap<Integer,Integer> count = new TwoKeyTreeCountMap<>();
        Integer taxA = Integer.valueOf(args[1]);
        Integer taxB = Integer.valueOf(args[2]);

        in.stream().forEach(kwd -> updateCount(count, kwd, taxA, taxB));

        for (Map.Entry<Integer, TreeCountMap<Integer>> e: count.entrySet())
        {
            for (Map.Entry<Integer,Long> e2: e.getValue().entrySet())
            {
                String t1 = (e.getKey()==Integer.MAX_VALUE)?"-":e.getKey().toString();
                String t2 = (e2.getKey()==Integer.MAX_VALUE)?"-":e2.getKey().toString();
                System.out.println(t1 + "\t" + t2 + "\t" + e2.getValue());
            }
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
}
