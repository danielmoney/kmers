package DataTypes;

import Compression.Compressor;
import Compression.IntCompressor;
import CountMaps.TreeCountMap;
import Counts.CountCompressor;
import Kmers.KmerWithData;
import Reads.ReadPos;
import Reads.ReadPosCompressor;
import Reads.ReadPosSetCompressor;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DataType<D,C>
{
    DataType(Compressor<D> dataCompressor, Compressor<C> collectionCompressor,
                    BiConsumer<C,C> merger, Collector<D,?,C> collector)
    {
        this.dataCompressor = dataCompressor;
        this.collectionCompressor = collectionCompressor;
        this.merger = merger;
        this.collector = collector;

        this.kmerReducer = (kwd1, kwd2) -> {merger.accept(kwd1.getData(), kwd2.getData()); return kwd1;};
    }


    public BiConsumer<C,C> getMerger()
    {
        return merger;
    }

    public Collector<D,?,C> getCollector()
    {
        return collector;
    }

    public Compressor<D> getDataCompressor()
    {
        return dataCompressor;
    }

    public Compressor<C> getCollectionCompressor()
    {
        return collectionCompressor;
    }

    public BinaryOperator<KmerWithData<C>> getKmerReducer()
    {
        return kmerReducer;
    }

    private Compressor<D> dataCompressor;
    private Compressor<C> collectionCompressor;
    private BiConsumer<C,C> merger;
    private Collector<D,?,C> collector;

    private BinaryOperator<KmerWithData<C>> kmerReducer;


    public static DataType<ReadPos, Set<ReadPos>> getReadPosInstance()
    {
        if (readPosInstance == null)
        {
            readPosInstance = new DataType<>(new ReadPosCompressor(), new ReadPosSetCompressor(),
                    (s1, s2) -> s1.addAll(s2),
                    Collectors.toSet());
        }
        return readPosInstance;
    }

    public static DataType<Integer, TreeCountMap<Integer>> getCountInstance()
    {
        if (countInstance == null)
        {
            countInstance = new DataType<>(new IntCompressor(), new CountCompressor(),
                    (c1, c2) -> c1.addAll(c2),
                    TreeCountMap.collector());
        }
        return countInstance;
    }

    private static DataType<ReadPos,Set<ReadPos>> readPosInstance = null;
    private static DataType<Integer,TreeCountMap<Integer>> countInstance = null;
}
