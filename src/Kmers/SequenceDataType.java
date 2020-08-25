package Kmers;

import DataTypes.DataType;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SequenceDataType implements DataType<Sequence>
{
    public Sequence decompress(ByteBuffer bb)
    {
        int len = bb.getInt();
        int l = (len - 1) / 4 + 1;

        byte[] sbytes = new byte[l];
        bb.get(sbytes);
        return Sequence.createUnchecked(Sequence.fromCompressed(sbytes, len));
    }

    public Sequence decompress(DataInput input) throws IOException
    {
        int len = input.readInt();
        int l = (len - 1) / 4 + 1;

        byte[] sbytes = new byte[l];
        input.readFully(sbytes);
        return Sequence.createUnchecked(Sequence.fromCompressed(sbytes, len));
    }

    public byte[] compress(Sequence seq)
    {
        return seq.compressedBytes();
    }

    public String toString(Sequence seq)
    {
        return seq.toString();
    }

    public Sequence fromString(String s)
    {
        return Sequence.createUnchecked(s.getBytes());
    }

    public int[] getID()
    {
        int[] id = new int[1];
        id[0] = 2053;
        return id;
    }


}
