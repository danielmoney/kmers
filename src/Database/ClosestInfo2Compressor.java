package Database;

import Compression.Compressor;
import Kmers.KmerWithData;
import Kmers.KmerWithDataCompressor;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class ClosestInfo2Compressor<D> implements Compressor<ClosestInfo2<D>>
{
    public ClosestInfo2Compressor(Compressor<D> dCompressor)
    {
        ciCompressor = new CICompressor<>(dCompressor);
    }

    public ClosestInfo2<D> decompress(ByteBuffer bb)
    {
        ClosestInfo2<D> closest = new ClosestInfo2();

        int num = bb.getInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(ciCompressor.decompress(bb));
        }

        return closest;
    }

    public ClosestInfo2<D> decompress(DataInput input) throws IOException
    {
        ClosestInfo2<D> closest = new ClosestInfo2();

        int num = input.readInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(ciCompressor.decompress(input));
        }

        return closest;
    }

    public byte[] compress(ClosestInfo2<D> ci)
    {
        ByteBuffer mbb = ByteBuffer.allocate(4);
        mbb.putInt(ci.getMatchedKmers().size());

        int c = 4;
        int s = 0;
        byte[][] temp = new byte[ci.getMatchedKmers().size()][];
        for (ClosestInfo2.CI<D> e: ci.getMatchedKmers())
        {
            temp[c] = ciCompressor.compress(e);
            s += temp[c].length;
            c++;
        }

        ByteBuffer bb = ByteBuffer.allocate(s);
        bb.put(mbb);
        for (int i = 0; i < temp.length; i++)
        {
            bb.put(temp[i]);
        }

        return bb.array();
    }

    public String toString(ClosestInfo2<D> ci)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(ci.getMinDist());
        for (ClosestInfo2.CI c: ci.getMatchedKmers())
        {
            sb.append(" | ");
            sb.append(ciCompressor.toString(c));
        }
        return sb.toString();
    }

    public ClosestInfo2<D> fromString(String s)
    {
        String[] parts = s.split(" | ");
        ClosestInfo2<D> closest = new ClosestInfo2<>();
        for (int i = 1; i < parts.length; i++)
        {
            closest.add(ciCompressor.fromString(parts[i]));
        }
        return closest;
    }

    public int getID()
    {
        return 8192 + ciCompressor.getID();
    }

    private Compressor<ClosestInfo2.CI<D>> ciCompressor;


    public static class CICompressor<D> implements Compressor<ClosestInfo2.CI<D>>
    {
        public CICompressor(Compressor<D> dCompressor)
        {
            kCompressor = new KmerWithDataCompressor<>(dCompressor);
        }

        public ClosestInfo2.CI<D> decompress(ByteBuffer bb)
        {
            return new ClosestInfo2.CI<>(kCompressor.decompress(bb), bb.get());
        }

        public ClosestInfo2.CI<D> decompress(DataInput input) throws IOException
        {
            return new ClosestInfo2.CI<>(kCompressor.decompress(input),input.readByte());
        }

        public byte[] compress(ClosestInfo2.CI<D> c)
        {
            byte[] temp = kCompressor.compress(c.getKWD());
            byte[] ret = new byte[temp.length+1];
            System.arraycopy(temp,0,ret,0,temp.length);
            ret[temp.length] = c.getDist();
            return ret;
        }

        public String toString(ClosestInfo2.CI<D> c)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(c.getDist());
            sb.append("\t");
            sb.append(kCompressor.toString(c.getKWD()));
            return sb.toString();
        }

        public ClosestInfo2.CI<D> fromString(String s)
        {
            String[] parts = s.split("\t",2);
            return new ClosestInfo2.CI<D>(kCompressor.fromString(parts[1]), Byte.parseByte(parts[0]));
        }

        public int getID()
        {
            return 4096 + kCompressor.getID();
        }

        private Compressor<KmerWithData<D>> kCompressor;
    }
}
