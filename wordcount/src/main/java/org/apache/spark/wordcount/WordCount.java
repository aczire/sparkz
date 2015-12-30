/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.List;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function3;
//import org.apache.spark.wordcount.State;
//import org.apache.spark.wordcount.StateSpec;
import org.apache.spark.ex.util.*;
import org.apache.spark.api.java.JavaPairRDD;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setAppName("Word Count");
		Duration batchDuration = new Duration(1000);
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				batchDuration);
		jssc.checkpoint(".");
		final int threshold = Integer.parseInt(args[0]);

		
		// Initial state RDD input to mapWithState
		@SuppressWarnings("unchecked")
		List<Tuple2<String, Integer>> tuples = Arrays.asList(
				new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("world", 1));
		JavaPairRDD<String, Integer> initialRDD = jssc.sparkContext()
				.parallelizePairs(tuples);

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				"127.0.0.1", 37337, StorageLevels.MEMORY_AND_DISK_SER_2);

		// split each document into words
		JavaDStream<String> tokenized = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(SPACE.split(s));
					}
				});

		// count the occurrence of each word
		JavaPairDStream<String, Integer> words = tokenized
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		// Update the cumulative count function
		final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> call(String word,
					Optional<Integer> one, State<Integer> state) {
				int sum = one.or(0) + (state.exists() ? state.get() : 0);
				Tuple2<String, Integer> output = new Tuple2<String, Integer>(
						word, sum);
				state.update(sum);
				return output;
			}
		};
/*
		// DStream made of get cumulative counts that get updated in every batch
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = wordsDstream
				.mapWithState(StateSpec.function(mappingFunc).initialState(
						initialRDD));
*/
		// count the occurrence of each word
		JavaPairDStream<String, Integer> counts = words
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		// filter out words with less than threshold occurrences
		JavaPairDStream<String, Integer> filtered = counts
				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Integer> tup) {
						return tup._2() >= threshold;
					}
				});

		// count characters
		JavaPairDStream<Character, Integer> charCounts = filtered
				.flatMap(
						new FlatMapFunction<Tuple2<String, Integer>, Character>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Iterable<Character> call(
									Tuple2<String, Integer> s) {
								Collection<Character> chars = new ArrayList<Character>(
										s._1().length());
								for (char c : s._1().toCharArray()) {
									chars.add(c);
								}
								return chars;
							}
						})
				.mapToPair(new PairFunction<Character, Character, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Character, Integer> call(Character c) {
						return new Tuple2<Character, Integer>(c, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		System.out.println("Wordcount: ");
		counts.print();
		System.out.println("Wordcount over threshold: ");
		filtered.print();
		System.out.println("Character Frequency: ");
		charCounts.print();

		jssc.start();
		jssc.awaitTermination();

		jssc.close();

	}
}
