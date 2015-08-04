package com.shn.dlp.sid.entries;

import java.io.IOException;
import java.io.DataInputStream;

final class CommonTermCellFactory extends CellFactory
{
    @Override
    public IndexEntry read(final DataInputStream in, final int termLength) throws IOException {
        return new CommonTermCell((Cell)super.read(in, termLength));
    }
}

