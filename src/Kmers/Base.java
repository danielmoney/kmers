package Kmers;

import Exceptions.InvalidBaseException;

public enum Base
{
    A ((byte) 0),
    C ((byte) 1),
    T ((byte) 2),
    G ((byte) 3);

    Base(byte i)
    {
        this.pos = i;
    }

    public byte pos()
    {
        return pos;
    }

    public static Base fromCharacterByte(Byte b) throws InvalidBaseException
    {
        switch (b)
        {
            case 'A':
            case 'a':
                return Base.A;
            case 'C':
            case 'c':
                return Base.C;
            case 'T':
            case 't':
                return Base.T;
            case 'G':
            case 'g':
                return Base.G;
            default:
                throw new InvalidBaseException();
        }
    }


    public static Base fromByte(Byte b) throws InvalidBaseException
    {
        switch (b)
        {
            case 0:
                return Base.A;
            case 1:
                return Base.C;
            case 2:
                return Base.T;
            case 3:
                return Base.G;
            default:
                throw new InvalidBaseException();
        }
    }

    private final byte pos;
}
