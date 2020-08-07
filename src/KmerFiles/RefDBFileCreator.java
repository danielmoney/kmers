package KmerFiles;

import Compression.IntCompressor;
import CountMaps.TreeCountMap;

import java.io.File;
import java.io.IOException;

public class RefDBFileCreator extends FileCreator<Integer,TreeCountMap<Integer>>
{
    public RefDBFileCreator(File dbFileTemp, File indexFileTemp,
                            int keyLength, int maxKmerLength, int cacheSize) throws IOException
    {
        super(dbFileTemp, indexFileTemp, keyLength, maxKmerLength, cacheSize,
                new IntCompressor(),
                new CountCompressor(),
                TreeCountMap.collector());
    }

}
