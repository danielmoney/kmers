package Reads;

import DataTypes.DataType;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class ReadPosDataType implements DataType<ReadPos>
{
    public ReadPosDataType(String seperator)
    {
        this.seperator = seperator;
    }

    public ReadPosDataType()
    {
        this(":");
    }

    public byte[] compress(ReadPos rp)
    {
        ByteBuffer bb = ByteBuffer.allocate(6);
        bb.putInt(rp.getRead());
        bb.putShort(rp.getPos());
        return bb.array();
    }

//    public ReadPos decompress(byte[] data)
//    {
//        ByteBuffer bb = ByteBuffer.wrap(data);
//        return decompress(bb);
//    }

    public ReadPos decompress(ByteBuffer bb)
    {
        return new ReadPos(bb.getInt(), bb.getShort());
    }

    public ReadPos decompress(DataInput input) throws IOException
    {
        return new ReadPos(input.readInt(), input.readShort());
    }

    public String toString(ReadPos rp)
    {
        return rp.getRead() + seperator + rp.getPos();
    }

    public ReadPos fromString(String s)
    {
        String[] parts = s.split(Pattern.quote(seperator));
        return new ReadPos(Integer.valueOf(parts[0]), Short.valueOf(parts[1]));
    }

    public int[] getID()
    {
        int[] id = new int[1];
        id[0] = 2052;
        return id;
    }

    private String seperator;
}

