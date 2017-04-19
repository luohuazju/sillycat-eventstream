package com.sillycat.sillycateventstream;

import com.sillycat.sillycateventstream.apps.JavaKafkaWordCount;

public class ExecutorApp {

	public static void main(String[] args) {

		System.out.println("asdfasdf");
		
		try {
			JavaKafkaWordCount.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
