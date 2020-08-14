package KmerFiles;

import DataTypes.DataType;
import Reads.ReadPos;
import Reads.ReadPosCompressor;
import Reads.ReadPosSetCompressor;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class ReadDBFileCreator extends FileCreator<ReadPos, Set<ReadPos>>
{
    public ReadDBFileCreator(File dbFileTemp,
                             int keyLength, int maxKmerLength, int cacheSize) throws IOException
    {
        super(dbFileTemp, keyLength, maxKmerLength, cacheSize, DataType.getReadPosInstance());
//            new ReadPosCompressor(),
//            new ReadPosSetCompressor(),
//            Collectors.toSet());
    }
}
