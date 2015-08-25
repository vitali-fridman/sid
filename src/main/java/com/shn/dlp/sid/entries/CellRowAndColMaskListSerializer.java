package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.mapdb.Serializer;

public class CellRowAndColMaskListSerializer extends Serializer<ArrayList<CellRowAndColMask>> {

	@Override
	public void serialize(DataOutput out, ArrayList<CellRowAndColMask> list) throws IOException {
		out.writeInt(list.size());
		for (CellRowAndColMask entry : list) {
			out.writeInt(entry.getRow());
			out.writeInt(entry.getMask());
		}
		
	}

	@Override
	public ArrayList<CellRowAndColMask> deserialize(DataInput in, int available) throws IOException {
		int  size = in.readInt();
		ArrayList<CellRowAndColMask> list = new ArrayList<CellRowAndColMask>(size);
		for (int i=0; i<size; i++) {
			list.add(CellRowAndColMask.create(in.readInt(), in.readInt()));
		}
		return list;
	}

}
