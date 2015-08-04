package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.mapdb.Serializer;

public class CellLocationListSerializer extends Serializer<ArrayList<CellLocation>> {

	@Override
	public void serialize(DataOutput out, ArrayList<CellLocation> list) throws IOException {
		out.writeInt(list.size());
		for (CellLocation location : list) {
			out.writeInt(location.getRow());
			out.writeByte(location.getColumn());;
		}
		
	}

	@Override
	public ArrayList<CellLocation> deserialize(DataInput in, int available) throws IOException {
		int size = in.readInt();
		ArrayList<CellLocation> list = new ArrayList<CellLocation>(size);
		for (int i=0; i< size; i++) {
			list.add(new CellLocation(in.readInt(), in.readByte()));
		}
		return list;
	}
	
	
}
