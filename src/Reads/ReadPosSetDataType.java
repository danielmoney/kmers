package Reads;

import Compression.Compressor;
import DataTypes.MergeableDataType;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ReadPosSetDataType implements MergeableDataType<Set<ReadPos>>
{
    public byte[] compress(Set<ReadPos> set)
    {
        int l = set.size() * 6 + 2;
        ByteBuffer b = ByteBuffer.allocate(l);

        b.putShort((short) set.size());

        for (ReadPos rp: set)
        {
            b.putInt(rp.getRead());
            b.putShort(rp.getPos());
        }

        return b.array();
    }

    private Set<ReadPos> decompress(byte[] bytes)
    {
        ByteBuffer b = ByteBuffer.wrap(bytes);

        int num = b.getShort();
        Set<ReadPos> map = new HashSet<>();

        for (int i = 0; i < num; i++)
        {
            int id = b.getInt();
            short c = b.getShort();
            map.add(new ReadPos(id,c));
        }

        return map;
    }

    public Set<ReadPos> decompress(ByteBuffer bb)
    {
        short cnum = bb.getShort();

        ByteBuffer countbytes = ByteBuffer.allocate(cnum*6+2);
        countbytes.putShort(cnum);

        byte[] cb = new byte[cnum*6];
        bb.get(cb);
        countbytes.put(cb);

        return decompress(countbytes.array());
    }

    public Set<ReadPos> decompress(DataInput input) throws IOException
    {
        short cnum = input.readShort();

        ByteBuffer countbytes = ByteBuffer.allocate(cnum*5+2);
        countbytes.putShort(cnum);

        byte[] cb = new byte[cnum*6];
        input.readFully(cb);
        countbytes.put(cb);

        return decompress(countbytes.array());
    }

    public String toString(Set<ReadPos> set)
    {
        return set.stream().map(rp -> rp.toString()).collect(Collectors.joining(" "));
    }

    public Set<ReadPos> fromString(String s)
    {
        TreeSet<ReadPos> ret = new TreeSet<>();
        String[] parts = s.split(" ");
        for (String p: parts)
        {
            String[] ps = s.split(":");
            ret.add(new ReadPos(Integer.valueOf(ps[0]), Short.valueOf(ps[1])));
        }
        return ret;
    }

    public BiConsumer<Set<ReadPos>, Set<ReadPos>> getMerger()
    {
        return (s1, s2) -> s1.addAll(s2);
    }

    public int getID()
    {
        return 1026;
    }
}
