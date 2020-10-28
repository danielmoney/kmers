package Utils;

import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataType;
import DataTypes.MergeableDataType;
import DataTypes.SetDataType;
import KmerFiles.KmerFile;
import Kmers.KmerWithData;
import Kmers.KmerWithDataDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class Extract
{
    public static void main(String[] args) throws IOException
    {
        File f = new File(args[0]);
        KmerFile.MetaData meta = KmerFile.getMetaData(f);

        MergeableDataType<?> dt = null;

        MergeableDataType<Set<ReadPos>> readsDT = new SetDataType<>(new ReadPosDataType());
        //if (dataID == 1026) // If reads file against db
        if (Arrays.equals(meta.dataID, readsDT.getID()))
        {
            dt = readsDT;
        }

//        MergeableDataType<TreeCountMap<Integer>> referenceDT = new CountDataType("x","|");
        MergeableDataType<TreeCountMap<Integer>> referenceDT = new CountDataType();
        if (Arrays.equals(meta.dataID, referenceDT.getID()))
        {
            dt = referenceDT;
        }

        print(f,dt);
    }

    private static <D> void print(File f, MergeableDataType<D> dt) throws IOException
    {
        KmerFile<D> in = new KmerFile<>(f,dt);
        DataType<KmerWithData<D>> kDT = new KmerWithDataDataType<>(dt);
        in.allKmers().forEach(kwd -> System.out.println(kDT.toString(kwd)));
    }
}