package KmerFiles;

import Compression.IntCompressor;
import CountMaps.TreeCountMap;
import DataTypes.DataType;

import java.io.File;
import java.io.IOException;

public class RefDBFileCreator extends FileCreator<Integer,TreeCountMap<Integer>>
{
    public RefDBFileCreator(File dbFileTemp,
                            int keyLength, int maxKmerLength, int cacheSize) throws IOException
    {
        super(dbFileTemp, keyLength, maxKmerLength, cacheSize, DataType.getCountInstance());
//                new IntCompressor(),
//                new CountCompressor(),
//                TreeCountMap.collector());
    }

}
