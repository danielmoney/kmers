package DataTypes;

import CountMaps.TreeCountMap;
import Counts.CountDataType;
import Kmers.KmerDiff;
import Kmers.KmerDiffDataType;
import Kmers.KmerWithDataDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;

import java.util.Set;

public class ResultsDataType<S,M> extends KmerWithDataDataType<DataPair<S, Set<DataPair<KmerDiff,M>>>>
{
    public ResultsDataType(DataType<S> sdt, DataType<M> mdt)
    {
        super(new DataPairDataType<>(
                sdt,
                new SetDataType<>(
                        new DataPairDataType<>(
                                new KmerDiffDataType(),
                                mdt,
                                "|"
                        ),
                        " "
                ),
        "\t"));
    }

    public static ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> getReadReferenceInstance()
    {
        DataType<Set<ReadPos>> readsDT = new SetDataType<>(new ReadPosDataType(),"|");
        DataType<TreeCountMap<Integer>> referenceDT = new CountDataType("x","|");
        return new ResultsDataType<>(readsDT,referenceDT);
    }

    public static ResultsDataType<TreeCountMap<Integer>, TreeCountMap<Integer>> getReferenceReferenceInstance()
    {
        DataType<TreeCountMap<Integer>> referenceDT = new CountDataType("x","|");
        return new ResultsDataType<>(referenceDT,referenceDT);
    }
}
