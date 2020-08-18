package Kmers;

import Compression.Compressor;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class KmerWithDataCompressor<D> implements Compressor<KmerWithData<D>>
{
    public KmerWithDataCompressor(Compressor<D> dataCompressor)
    {
        this.dataCompressor = dataCompressor;
    }

//    public KmerWithData<D> decompress(byte[] bytes)
//    {
//        int len = bytes[0];
//        int l = (len - 1) / 4 + 1;
//
//        Kmer k = Kmer.createFromCompressed(Arrays.copyOfRange(bytes,0,l+1));
//        D d = dataCompressor.decompress(Arrays.copyOfRange(bytes,l+1,bytes.length));
//
//        return new KmerWithData<>(k,d);
//    }

    public KmerWithData<D> decompress(ByteBuffer bb)
    {
        byte len = bb.get();
        int l = (len - 1) / 4 + 1;

        byte[] kbytes = new byte[l+1];
        kbytes[0] = len;
        bb.get(kbytes,1,kbytes.length-1);
        Kmer k = Kmer.createFromCompressed(kbytes);
        D d = dataCompressor.decompress(bb);
        return new KmerWithData<>(k,d);
    }

    public KmerWithData<D> decompress(DataInput input) throws IOException
    {
        byte len = input.readByte();
        int l = (len - 1) / 4 + 1;

        byte[] kbytes = new byte[l+1];
        kbytes[0] = len;
        input.readFully(kbytes,1, kbytes.length-1);
        Kmer k = Kmer.createFromCompressed(kbytes);
        D d = dataCompressor.decompress(input);
        return new KmerWithData<>(k,d);
    }

    public byte[] compress(KmerWithData<D> kwd)
    {
        byte[] kb = kwd.getKmer().compressedBytes();
        byte[] db = dataCompressor.compress(kwd.getData());

        byte[] b = new byte[kb.length + db.length];
        System.arraycopy(kb,0,b,0,kb.length);
        System.arraycopy(db,0,b,kb.length,db.length);

        return b;
    }

    public String toString(KmerWithData<D> kwd)
    {
        return kwd.toString();
    }

    public KmerWithData<D> fromString(String s)
    {
        String[] parts = s.split("\t",1);
        Kmer k = Kmer.createUnchecked(parts[0].getBytes());
        D d = dataCompressor.fromString(parts[1]);
        return new KmerWithData<>(k,d);
    }

    public int getID()
    {
        return 2048 + dataCompressor.getID();
    }

    Compressor<D> dataCompressor;
}
