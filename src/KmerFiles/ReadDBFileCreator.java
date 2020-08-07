package KmerFiles;

import Reads.ReadPos;
import Reads.ReadPosCompressor;
import Reads.ReadPosSetCompressor;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class ReadDBFileCreator extends FileCreator<ReadPos, Set<ReadPos>>
{
    public ReadDBFileCreator(File dbFileTemp, File indexFileTemp,
                             int keyLength, int maxKmerLength, int cacheSize) throws IOException
    {
        super(dbFileTemp, indexFileTemp, keyLength, maxKmerLength, cacheSize,
            new ReadPosCompressor(),
            new ReadPosSetCompressor(),
            Collectors.toSet());
    }
}
