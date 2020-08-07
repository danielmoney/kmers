package Kmers;

import Exceptions.InvalidBaseException;

import java.util.Arrays;

public class Kmer implements Comparable<Kmer>
{
    public Kmer(String s) throws InvalidBaseException
    {
//        this(s.substring(s.lastIndexOf('-')+1).getBytes());
        this(s.substring(0,s.indexOf('-')).getBytes());
    }

    public Kmer(byte[] chars) throws InvalidBaseException
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

    private Kmer()
    {

    }

    public Kmer(Kmer kmer)
    {
        chars = new byte[kmer.chars.length];
        System.arraycopy(kmer.chars,0,chars,0,kmer.chars.length);
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
        byte[] r = new byte[l];
        int c = 1;
        byte cb = 0;
        int cc = 0;
        r[0] = (byte) chars.length;
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

    public Kmer changePosition(int pos, byte val)
    {
            Kmer newkmer = new Kmer();
            newkmer.chars = Arrays.copyOf(chars,chars.length);
            newkmer.chars[pos] = val;
            return newkmer;
    }

    public byte[] getRawBytes()
    {
        return Arrays.copyOf(chars,chars.length);
    }

    public int length()
    {
        return chars.length;
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

    public boolean equals(Object o)
    {
        if (o instanceof Kmer)
        {
            Kmer k = (Kmer) o;
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

    public int compareTo(Kmer k)
    {
//        if (k.chars.length != chars.length)
//        {
//            if (chars.length < k.chars.length)
//            {
//                return -1;
//            }
//            else
//            {
//                return 1;
//            }
//        }
//
//        for (int i = 0; i < chars.length; i++)
//        {
//            if (chars[i] < k.chars[i])
//            {
//                return -1;
//            }
//            if (chars[i] > k.chars[i])
//            {
//                return 1;
//            }
//        }
//        return 0;

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
        int len = bytes[0];
        byte[] b = new byte[len];

        int l = (len - 1) / 4 + 1;
        for (int i = 0; i < l; i++)
        {
            byte cb = bytes[i+1];

            for (int j = 0; j < 4; j++)
            {
                int p = i*4+(3-j);
                if (p < len)
                {
                    b[i * 4 + (3 - j)] = (byte) (cb & 3);
                }
                cb = (byte) (cb >> 2);
            }
        }

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

    protected byte[] chars;
    protected KmerStandard standard = KmerStandard.UNKNOWN;

    public enum KmerStandard
    {
        STANDARD,
        RC,
        UNKNOWN
    }
}
