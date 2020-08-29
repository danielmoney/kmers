package Kmers;

import DataTypes.DataType;
import Exceptions.InvalidBaseException;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class KmerDiffDataType implements DataType<KmerDiff>
{
    public byte[] compress(KmerDiff diff)
    {
        List<KmerDiff.Diff> diffs = diff.getDiffs();
        ByteBuffer bb = ByteBuffer.allocate(diffs.size()*2 + 1);
        bb.put((byte) diffs.size());
        for (KmerDiff.Diff d: diffs)
        {
            bb.put(d.getPosition());
            bb.put(d.getBase().pos());
        }
        return bb.array();
    }

    public KmerDiff decompress(ByteBuffer bb)
    {
        List<KmerDiff.Diff> diffs = new LinkedList<>();
        byte l = bb.get();
        for (byte i = 0; i < l; i++)
        {
            try
            {
                diffs.add(new KmerDiff.Diff(bb.get(), Base.fromByte(bb.get())));
            }
            catch (InvalidBaseException e)
            {
                // Hopefully shouldn't get here!
            }
        }

        return new KmerDiff(diffs);
    }

    public KmerDiff decompress(DataInput input) throws IOException
    {
        List<KmerDiff.Diff> diffs = new LinkedList<>();
        byte l = input.readByte();
        for (byte i = 0; i < l; i++)
        {
            try
            {
                diffs.add(new KmerDiff.Diff(input.readByte(), Base.fromByte(input.readByte())));
            }
            catch (InvalidBaseException e)
            {
                // Hopefully shouldn't get here!
            }
        }

        return new KmerDiff(diffs);
    }

    public String toString(KmerDiff diff)
    {
        return diff.toString();
    }

    public KmerDiff fromString(String s)
    {
        List<KmerDiff.Diff> diffs = new LinkedList<>();

        if (s.equals("-"))
        {
            return new KmerDiff(diffs);
        }

        int p = 0;
        int posstart = 0;
        while (p != s.length())
        {
            char c = s.charAt(p);
            if ((c > 47) && (c < 58))
            {
                p ++;
            }
            else
            {
                byte pos = Byte.parseByte(s.substring(posstart,p));
                posstart = p + 1;
                try
                {
                    Base base = Base.fromCharacterByte((byte) c);
                    diffs.add(new KmerDiff.Diff(pos,base));
                }
                catch (InvalidBaseException e)
                {
                    //Shouldn't get here... I hope
                }
                p++;
            }
        }

        return new KmerDiff(diffs);
    }

    public int[] getID()
    {
        int[] id = new int[1];
        id[0] = 2054;
        return id;
    }
}
