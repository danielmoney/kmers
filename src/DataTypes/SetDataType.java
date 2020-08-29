package DataTypes;

import Compression.SetCompressor;

import java.util.Set;
import java.util.function.BiConsumer;

public class SetDataType<D> extends SetCompressor<D> implements MergeableDataType<Set<D>>
{
    public SetDataType(DataType<D> dataCompressor)
    {
        super(dataCompressor);
    }

    public SetDataType(DataType<D> dataCompressor, String seperator)
    {
        super(dataCompressor, seperator);
    }

    public BiConsumer<Set<D>, Set<D>> getMerger()
    {
        return (s1, s2) -> s1.addAll(s2);
    }
}
