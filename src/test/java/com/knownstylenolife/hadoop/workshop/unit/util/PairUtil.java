package com.knownstylenolife.hadoop.workshop.unit.util;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.types.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class PairUtil {

	public static <T1, T2> List<String> toStrings(List<Pair<T1, T2>> list) {
		return PairUtil.toStrings(list,"\t");
	}
	
	public static <T1, T2> List<String> toStrings(List<Pair<T1, T2>> list, final String delimiter) {
		return Lists.transform(list, new Function<Pair<T1, T2>, String>() {
			public String apply(Pair<T1, T2> input) {
				return new StringBuilder()
				.append(input.getFirst().toString())
				.append(delimiter)
				.append(input.getSecond().toString()).toString();
			}
		});
	}
}
