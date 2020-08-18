package DataTypes;

import Compression.Compressor;
import Kmers.KmerWithData;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.stream.Collector;

//public class MergeableDataType<D>
//{
//    MergeableDataType(Compressor<D> compressor, BiConsumer<D,D> merger)
//    {
//        this.compressor = compressor;
//        this.merger = merger;
//
//        this.kmerReducer = (kwd1, kwd2) -> {merger.accept(kwd1.getData(), kwd2.getData()); return kwd1;};
//    }
//
//
//    public BiConsumer<D,D> getMerger()
//    {
//        return merger;
//    }
//
//    public Compressor<D> getCompressor()
//    {
//        return compressor;
//    }
//
//    public BinaryOperator<KmerWithData<D>> getKmerReducer()
//    {
//        return kmerReducer;
//    }
//
//    private Compressor<D> compressor;
//    private BiConsumer<D,D> merger;
//
//    private BinaryOperator<KmerWithData<D>> kmerReducer;
//}

public interface MergeableDataType<D> extends DataType<D>
{
    public BiConsumer<D,D> getMerger();

    public default BinaryOperator<KmerWithData<D>> getKmerReducer()
    {
        return (kwd1, kwd2) -> {getMerger().accept(kwd1.getData(), kwd2.getData()); return kwd1;};
    }
}
