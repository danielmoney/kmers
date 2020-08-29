package DataTypes;

import Compression.Compressor;
import Compression.MapCompressor;
import Compression.SetCompressor;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class MapDataType<K,V> extends MapCompressor<K,V> implements MergeableDataType<Map<K,V>>
{
    public MapDataType(DataType<K> keyDataType, DataType<V> valueDataType)
    {
        super(keyDataType,valueDataType,":"," ");
    }

    public MapDataType(DataType<K> keyDataType, Compressor<V> valueDataType,
                         String kvSeperator, String entrySeperator)
    {
        super(keyDataType,valueDataType,kvSeperator,entrySeperator);
    }

    public BiConsumer<Map<K,V>, Map<K,V>> getMerger()
    {
        return (m1, m2) -> m1.putAll(m2);
    }
}
