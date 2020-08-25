package Counts;

import CountMaps.TreeCountMapCompressor;
import Compression.IntCompressor;
import CountMaps.TreeCountMap;
import DataTypes.MergeableDataType;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

//Does not use TreeCountMapCompressor as it stores counts as longs and we don't need to

public class CountDataType implements MergeableDataType<TreeCountMap<Integer>>
{
    public byte[] compress(TreeCountMap<Integer> map)
    {
        int l = map.size() * 5 + 2;
        ByteBuffer b = ByteBuffer.allocate(l);

        b.putShort((short) map.size());

        for (Map.Entry<Integer,Long> e: map.entrySet())
        {
            byte c = (byte) Math.min(e.getValue(), 255);
            b.putInt(e.getKey());
            b.put(c);
        }

        return b.array();
    }

    private TreeCountMap<Integer> decompress(byte[] bytes)
    {
        ByteBuffer b = ByteBuffer.wrap(bytes);

        int num = b.getShort();
        TreeCountMap<Integer> map = new TreeCountMap<>();

        for (int i = 0; i < num; i++)
        {
            int id = b.getInt();
            int c = b.get();
            if (c < 0)
            {
                c = 256+c;
            }
            map.put(id,(long) c);
        }

        return map;
    }

    public TreeCountMap<Integer> decompress(ByteBuffer bb)
    {
        short cnum = bb.getShort();

        ByteBuffer countbytes = ByteBuffer.allocate(cnum*5+2);
        countbytes.putShort(cnum);

        byte[] cb = new byte[cnum*5];
        bb.get(cb);
        countbytes.put(cb);

        return decompress(countbytes.array());
    }

    public TreeCountMap<Integer> decompress(DataInput input) throws IOException
    {
        short cnum = input.readShort();

        ByteBuffer countbytes = ByteBuffer.allocate(cnum*5+2);
        countbytes.putShort(cnum);

        byte[] cb = new byte[cnum*5];
        input.readFully(cb);
        countbytes.put(cb);

        return decompress(countbytes.array());
    }

    public TreeCountMap<Integer> fromString(String s)
    {
        TreeCountMap<Integer> map = new TreeCountMap<>();
        String parts[] = s.split(" ");
        for (String p: parts)
        {
            String[] parts2 = p.split(":");
            map.put(Integer.parseInt(parts2[0]),Long.parseLong(parts2[1]));
        }
        return map;
    }

    public String toString(TreeCountMap<Integer> map)
    {
        return map.entrySet().stream().map(e2 -> e2.getKey() + ":" + e2.getValue())
                                .collect(Collectors.joining(" "));
    }

    public int[] getID()
    {
//        return 1024;
        int[] id = new int[1];
        id[0] = 2048;
        return id;
    }

    public BiConsumer<TreeCountMap<Integer>, TreeCountMap<Integer>> getMerger()
    {
        return (c1, c2) -> c1.addAll(c2);
    }
}
