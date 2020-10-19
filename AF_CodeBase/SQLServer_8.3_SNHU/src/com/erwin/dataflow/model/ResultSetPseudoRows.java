package com.erwin.dataflow.model;

import com.erwin.util.Pair;

public class ResultSetPseudoRows extends PseudoRows<ResultSet> {

	public ResultSetPseudoRows(ResultSet holder) {
		super(holder);
	}

	public Pair<Long, Long> getStartPosition() {
		return getHolder().getStartPosition();
	}

	public Pair<Long, Long> getEndPosition() {
		return getHolder().getEndPosition();
	}
}
