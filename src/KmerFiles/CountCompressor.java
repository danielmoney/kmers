package KmerFiles;

import Compression.Compressor;
import CountMaps.TreeCountMap;
import Kmers.Kmer;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

public class CountCompressor implements Compressor<TreeCountMap<Integer>>
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

    public TreeCountMap<Integer> decompress(byte[] bytes)
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
            map.put(id,c);
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
        return null;
    }

    public String toString(TreeCountMap<Integer> map)
    {
        return map.entrySet().stream().map(e2 -> e2.getKey() + ":" + e2.getValue())
                                .collect(Collectors.joining(" "));
    }
}
