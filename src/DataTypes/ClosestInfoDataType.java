package DataTypes;

import Database.ClosestInfo;
import Database.ClosestInfo2;
import Database.ClosestInfo2Compressor;
import Kmers.KmerWithData;

import java.util.stream.Collector;

public class ClosestInfoDataType<D> extends DataType<ClosestInfo2.CI<D>, ClosestInfo2<D>>
{
    public ClosestInfoDataType(DataType<?,D> kmerData)
    {
        super(
                new ClosestInfo2Compressor.CICompressor<>(kmerData.getCollectionCompressor()), //Compressor<D> dataCompressor,
                new ClosestInfo2Compressor<>(kmerData.getCollectionCompressor()), //Compressor<C> collectionCompressor,
                (c1,c2) -> c1.addAll(c2), //BiConsumer<C,C> merger,
                Collector.of(ClosestInfo2<D>::new,(c,s) -> c.add(s),(c1,c2) -> {c1.addAll(c2); return c1;}) //Collector<D,?,C> collector
        );
    }






}
