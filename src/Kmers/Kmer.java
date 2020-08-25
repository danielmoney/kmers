package Kmers;

import Exceptions.InvalidBaseException;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Kmer extends Sequence
{
    public Kmer(String s) throws InvalidBaseException
    {
        this(s.substring(s.lastIndexOf('-')+1).getBytes());
//        this(s.substring(0,s.indexOf('-')).getBytes());
    }

    public Kmer(byte[] chars) throws InvalidBaseException
    {
        super(chars);
    }

    public Kmer(Kmer kmer)
    {
        super(kmer);
    }

    public Kmer(Kmer kmer, String s) throws InvalidBaseException
    {
        byte[] replacechars = s.getBytes();
        for (int i = 0; i < replacechars.length; i++)
        {
            replacechars[i] = Base.fromCharacterByte(replacechars[i]).pos();
        }
        if (replacechars.length < kmer.chars.length)
        {
            byte[] newchars = new byte[kmer.chars.length];

            System.arraycopy(kmer.chars, 0, newchars, 0, kmer.chars.length - replacechars.length);
            System.arraycopy(replacechars, 0, newchars, kmer.chars.length - replacechars.length, replacechars.length);
            this.chars = newchars;
        }
        else
        {
            this.chars = replacechars;
        }
    }

    public int key(int num)
    {
        int key = 0;
        for (int i = 0; i < num; i++)
        {
            key = key * 4 + chars[i];
        }
        return key;
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
        int l = (chars.length - 1) / 4 + 1 + 1;
        ByteBuffer bb = ByteBuffer.allocate(l);
        bb.put((byte) chars.length);
        bb.put(cBytes());
        return bb.array();
    }

    public String toDBString(int length)
    {
        StringBuilder sb = new StringBuilder();

//        for (int i = 0; i < length - chars.length; i++)
//        {
//            sb.append('-');
//        }

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

        for (int i = 0; i < length - chars.length; i++)
        {
            sb.append('-');
        }

        return sb.toString();
    }

    public Kmer getRC()
    {
        Kmer k = Kmer.createUnchecked(rc(chars));
        if (standard == KmerStandard.STANDARD)
        {
            k.standard = KmerStandard.RC;
        }
        if (standard == KmerStandard.RC)
        {
            k.standard = KmerStandard.STANDARD;
        }
        return k;
    }

    public StandardKmer getStandardKmer()
    {
        return StandardKmer.createUnchecked(chars);
    }

    public boolean isStandard()
    {
        switch (standard)
        {
            case STANDARD:
                return true;
            case RC:
                return false;
            case UNKNOWN:
                byte[] rc = rc(chars);

                for (int i = 0; i < chars.length; i++)
                {
                    if (chars[i] < rc[i])
                    {
                        standard = KmerStandard.STANDARD;
                        return true;
                    }
                    if (chars[i] > rc[i])
                    {
                        standard = KmerStandard.RC;
                        return false;
                    }
                }

                standard = KmerStandard.STANDARD;
                return true;
        }
        System.out.println("I don't think we can get here!");
        return false;
    }

    public int dist(Kmer k)
    {
        int c = 0;
        for (int i = 0; i < chars.length; i++)
        {
            if (chars[i] != k.chars[i])
            {
                c++;
            }
        }
        return c;
    }

    public Kmer limitTo(int length)
    {
        if (length < chars.length)
        {
            byte[] newchars = new byte[length];
            System.arraycopy(chars, 0, newchars, 0, length);
            return Kmer.createUnchecked(newchars);
        }
        else
        {
            return this;
        }
    }

    public static Kmer createUnchecked(byte[] chars)
    {
        Kmer k = null;
        try
        {
            k = new Kmer(chars);
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }

    public static Kmer createUnchecked(Kmer old, String s)
    {
        Kmer k = null;
        try
        {
            if (old == null)
            {
                k = new Kmer(s);
            }
            else
            {
                k = new Kmer(old, s);
            }
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }

    public static Kmer createFromCompressed(byte[] bytes)
    {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int len = bb.get();

        byte[] seqBytes = new byte[bytes.length - 1];
        bb.get(seqBytes);
        byte[] b = fromCompressed(seqBytes, len);

        Kmer k = null;
        try
        {
            k = new Kmer(b);
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }

    private static final int MOD = 2^32-1;

    protected KmerStandard standard = KmerStandard.UNKNOWN;

    public enum KmerStandard
    {
        STANDARD,
        RC,
        UNKNOWN
    }
}
