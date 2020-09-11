package Kmers;

import Exceptions.InvalidBaseException;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Sequence implements Comparable<Sequence>
{
    public Sequence(String s) throws InvalidBaseException
    {
        this(s.substring(s.lastIndexOf('-')+1).getBytes());
    }

    public Sequence(byte[] chars) throws InvalidBaseException
    {
        if (chars[0] > 4)
        {
            for (int i = 0; i < chars.length; i++)
            {
                chars[i] = Base.fromCharacterByte(chars[i]).pos();
            }
        }
        else
        {
            for (int i = 0; i < chars.length; i++)
            {
                if (chars[i] > 4)
                {
                    throw new InvalidBaseException();
                }
            }
        }
        this.chars = chars;
    }

    public Sequence(Sequence kmer)
    {
        chars = new byte[kmer.chars.length];
        System.arraycopy(kmer.chars,0,chars,0,kmer.chars.length);
    }

    protected Sequence()
    {

    }

    protected byte[] rc(byte[] orig)
    {
        byte[] nb = new byte[orig.length];

        for (int i = 0; i < orig.length; i++)
        {
            try
            {
                switch (Base.fromByte(orig[i]))
                {
                    case A:
                        nb[nb.length - i - 1] = Base.T.pos();
                        break;
                    case C:
                        nb[nb.length - i - 1] = Base.G.pos();
                        break;
                    case T:
                        nb[nb.length - i - 1] = Base.A.pos();
                        break;
                    case G:
                        nb[nb.length - i - 1] = Base.C.pos();
                        break;
                }
            }
            catch (InvalidBaseException ex)
            {
                // SHOULD NEVER GET HERE
            }
        }

        return nb;
    }

    public byte[] compressedBytes()
    {
        int l = (chars.length - 1) / 4 + 1 + 4;
        ByteBuffer bb = ByteBuffer.allocate(l);
        bb.putInt(chars.length);
        bb.put(cBytes());
        return bb.array();
    }

    protected byte[] cBytes()
    {
        int l = (chars.length - 1) / 4 + 1;
        byte[] r = new byte[l];
        int c = 0;
        byte cb = 0;
        int cc = 0;
        for (int i = 0; i < chars.length; i++)
        {
            cb = (byte) ((cb << 2) + chars[i]);
            cc++;
            if (cc == 4)
            {
                r[c] = cb;
                c++;
                cc = 0;
                cb = 0;
            }
        }
        if (cc != 0)
        {
            cb = (byte) (cb << ((4 - cc) * 2));
            r[c] = cb;
        }
        return r;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < chars.length; i++)
        {
            try
            {
                sb.append(Base.fromByte(chars[i]));
            }
            catch (InvalidBaseException ex)
            {
                // SHOULD NEVER GET HERE
            }
        }


        return sb.toString();
    }

    public byte[] getRawBytes()
    {
        //return Arrays.copyOf(chars,chars.length);
        return chars;
    }

    public int length()
    {
        return chars.length;
    }


    public boolean equals(Object o)
    {
        if (o instanceof Sequence)
        {
            Sequence k = (Sequence) o;
            if (k.chars.length != chars.length)
            {
                return false;
            }
            return Arrays.equals(chars, k.chars);
        }
        else
        {
            return false;
        }
    }

    public int compareTo(Sequence k)
    {
        for (int i = 0; i < Math.min(chars.length, k.chars.length); i++)
        {
            if (chars[i] < k.chars[i])
            {
                return -1;
            }
            if (chars[i] > k.chars[i])
            {
                return 1;
            }
        }
        if (chars.length < k.chars.length)
        {
            return -1;
        }
        if (chars.length > k.chars.length)
        {
            return 1;
        }
        return 0;
    }

    public int hashCode()
    {
        int h = 0;
        for (int i = 0; i < chars.length; i++)
        {
            h *= 4;
            h += chars[i];
            h = h % MOD;
        }
        return h;
    }

    public static Sequence createUnchecked(byte[] chars)
    {
        Sequence k = null;
        try
        {
            k = new Sequence(chars);
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }

    public static Sequence createFromCompressed(byte[] bytes)
    {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int len = bb.getInt();

        byte[] seqBytes = new byte[bytes.length - 4];
        bb.get(seqBytes);
        byte[] b = fromCompressed(seqBytes, len);

        Sequence k = null;
        try
        {
            k = new Sequence(b);
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }

    public static byte[] fromCompressed(byte[] bytes, int len)
    {
        byte[] b = new byte[len];

        int l = (len - 1) / 4 + 1;
        for (int i = 0; i < l; i++)
        {
            byte cb = bytes[i];

            for (int j = 0; j < 4; j++)
            {
                int p = i*4+(3-j);
                if (p < len)
                {
                    b[p] = (byte) (cb & 3);
                }
                cb = (byte) (cb >> 2);
            }
        }

        return b;
    }

    private static final int MOD = 2^32-1;

    protected byte[] chars;
}
