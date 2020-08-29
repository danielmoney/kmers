package DataTypes;

import CountMaps.TreeCountMap;
import Counts.CountDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;

import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DataCollector<D,C>
{
//    DataCollector(Compressor<D> dataCompressor, Compressor<C> collectionCompressor,
//                  BiConsumer<C,C> merger, Collector<D,?,C> collector)
    public DataCollector(DataType<D> dataType, MergeableDataType<C> collectedType,
                         Collector<D,?,C> collector)
    {
        this.dataType = dataType;
        this.collectedType = collectedType;
        this.collector = collector;
//        super(collectionCompressor, merger);
//        this.dataCompressor = dataCompressor;
//        //this.collectionCompressor = collectionCompressor;
//        //this.merger = merger;
//        this.collector = collector;

        //this.kmerReducer = (kwd1, kwd2) -> {merger.accept(kwd1.getData(), kwd2.getData()); return kwd1;};
    }

//
//    public BiConsumer<C,C> getMerger()
//    {
//        return merger;
//    }

    public Collector<D,?,C> getCollector()
    {
        return collector;
    }

    public DataType<D> getDataDataType()
    {
        return dataType;
    }

    public MergeableDataType<C> getCollectionDataType()
    {
        return collectedType;
    }

//    private Compressor<D> dataCompressor;
//    private Compressor<C> collectionCompressor;
//    private BiConsumer<C,C> merger;

    private DataType<D> dataType;
    private MergeableDataType<C> collectedType;
    private Collector<D,?,C> collector;

//    private BinaryOperator<KmerWithData<C>> kmerReducer;


    public static DataCollector<ReadPos, Set<ReadPos>> getReadPosInstance()
    {
        if (readPosInstance == null)
        {
            readPosInstance = new DataCollector<>(new ReadPosDataType(), new SetDataType<>(new ReadPosDataType()),
                    Collectors.toSet());
        }
        return readPosInstance;
    }

    public static DataCollector<Integer, TreeCountMap<Integer>> getCountInstance()
    {
        if (countInstance == null)
        {
            countInstance = new DataCollector<>(new IntDataType(), new CountDataType(),
                    TreeCountMap.collector());
        }
        return countInstance;
    }

    private static DataCollector<ReadPos,Set<ReadPos>> readPosInstance = null;
    private static DataCollector<Integer,TreeCountMap<Integer>> countInstance = null;
}
