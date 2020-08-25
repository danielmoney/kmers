package DataTypes;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataPairDataType<A,B> implements DataType<DataPair<A,B>>
{
    public DataPairDataType(DataType<A> aDataType, DataType<B> bDataType)
    {
        this.aDataType = aDataType;
        this.bDataType = bDataType;
    }

    public byte[] compress(DataPair<A,B> dp)
    {
        byte[] aBytes = aDataType.compress(dp.getA());
        byte[] bBytes = bDataType.compress(dp.getB());

        byte[] bytes = new byte[aBytes.length + bBytes.length];
        System.arraycopy(aBytes,0,bytes,0,aBytes.length);
        System.arraycopy(bBytes,0,bytes,aBytes.length,bBytes.length);
        return bytes;
    }

    public DataPair<A,B> decompress(ByteBuffer bb)
    {
        return new DataPair<>(aDataType.decompress(bb), bDataType.decompress(bb));
    }

    public DataPair<A,B> decompress(DataInput input) throws IOException
    {
        return new DataPair<>(aDataType.decompress(input), bDataType.decompress(input));
    }

    public String toString(DataPair<A,B> dp)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(aDataType.toString(dp.getA()));
        sb.append(" ~ ");
        sb.append(bDataType.toString(dp.getB()));
        return sb.toString();
    }

    public DataPair<A,B> fromString(String s)
    {
        String parts[] = s.split(" ~ ");
        return new DataPair<>(aDataType.fromString(parts[0]), bDataType.fromString(parts[1]));
    }

    public int[] getID()
    {
        int[] aID = aDataType.getID();
        int[] bID = bDataType.getID();
        int[] id = new int[aID.length + bID.length + 1];
        id[0] = 2050;
        System.arraycopy(aID,0,id,1,aID.length);
        System.arraycopy(bID,0,id,aID.length+1,bID.length);
        return id;
    }

    private DataType<A> aDataType;
    private DataType<B> bDataType;

}
