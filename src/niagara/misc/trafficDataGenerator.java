package niagara.misc;

/*******************************************************************************
 * NIAGARA -- Net Data Management System
 * 
 * Copyright (c) Computer Sciences Department, University of Wisconsin --
 * Madison All Rights Reserved.
 * 
 * Permission to use, copy, modify and distribute this software and its
 * documentation is hereby granted, provided that both the copyright notice and
 * this permission notice appear in all copies of the software, derivative works
 * or modified versions, and any portions thereof, and that both notices appear
 * in supporting documentation.
 * 
 * THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY OF
 * WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS " AS IS"
 * CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES
 * WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.
 * 
 * This software was developed with support by DARPA through Rome Research
 * Laboratory Contract No. F30602-97-2-0247.
 ******************************************************************************/

/*
 * Traffic Data Stream Generator
 * RJFM, 2008.09.22
 * */

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

public class trafficDataGenerator {

	
	public static void main(String args[]) {

		int detectorsPerSegment = 40, numberOfSegments = 9, hours = 18;

		// This is proven to work.
		//int detectorsPerSegment = 2, numberOfSegments = 5, hours = 1;
	
		String outputFile = "E:\\eclipse_workspace\\latte\\Streams\\trafficDataStream.xml";
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, d, MMM yyyy hh:mm:ss");

		Calendar startTime = Calendar.getInstance(); // / CALENDAR
		startTime.set(2008, 2, 15, 6, 0, 0);

		getData(args, detectorsPerSegment, numberOfSegments, hours, outputFile);

		showData(detectorsPerSegment, numberOfSegments, hours, outputFile);

		System.out.println(writeToFile(hours, outputFile, sdf, startTime, detectorsPerSegment,
				numberOfSegments));

	}

	
	private static int writeToFile(int hours, String outputFile,
			SimpleDateFormat sdf, Calendar startTime, int detectorsPerSegment,
			int numberOfSegments) {
		int volume, speed, occupancy, error;
		int counter = 0;
		try {

			FileWriter file = new FileWriter(outputFile);
			BufferedWriter out = new BufferedWriter(file);

			out.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n");
			out
					.write("<niagara:stream xmlns:niagara=\"http://www.cse.ogi.edu/dot/niagara\" xmlns:punct=\"http://www.cse.ogi.edu/dot/niagara/punct\">\n");
			for (int i = 0; i < hours * 60 * 3; i++) {
				out.write("<detectors>\n");
				out.write("<time_t>" + startTime.getTimeInMillis()
						+ "</time_t>\n");
				out.write("<time_s>" + sdf.format(startTime.getTime())
						+ "</time_s>\n");

				Random generator = new Random();

				for (int j = 0; j < numberOfSegments; j++) {
					for (int k = 0; k < detectorsPerSegment; k++) {
						out.write("<detector>");
						out.write("<detector_id>" + (1000 + 100 * j + k)
								+ "</detector_id>");
						out.write("<segment_id>" + (j + 1) + "</segment_id>");

						volume = generator.nextInt(10);
						speed = generator.nextInt(60) + 5;
						occupancy = generator.nextInt(90) + 10;
						error = generator.nextInt(100);

						if (error > 5) {

							out.write("<volume>" + volume + "</volume>");
							out.write("<speed>" + speed + "</speed>");
							out.write("<occupancy>" + occupancy
									+ "</occupancy>");
						} else {
							out.write("<volume>" + volume + "</volume>");
							out.write("<speed>0</speed>");
							out.write("<occupancy>" + occupancy
									+ "</occupancy>");
						}
						out.write("</detector>\n");
						counter++;
					}
				}

				out.write("</detectors>\n");
				out.write("<punct:detectors>\n");
				out.write("<time_t>" + startTime.getTimeInMillis()
						+ "</time_t>\n");
				out.write("<time_s>*</time_s>");
				out.write("<detector>");
				out.write("<detector_id>*</detector_id>");
				out.write("<segment_id>*</segment_id>");
				out.write("<volume>*</volume>");
				out.write("<speed>*</speed>");
				out.write("<occupancy>*</occupancy>");
				out.write("</detector>");

				out.write("</punct:detectors>\n");
				startTime.add(Calendar.SECOND, 20);

			}
			out.write("</niagara:stream>");
			out.close();
			return counter;
		} catch (Exception e) {
			System.out.println("Exception: " + e);
			return 0;
		}
	}

	private static void showData(int detectorsPerSegment, int numberOfSegments,
			int hours, String outputFile) {
		System.out.println("Detectors per segment: " + detectorsPerSegment);
		System.out.println("Segments: " + numberOfSegments);
		System.out.println("Hours: " + hours);
		System.out.println("Output file: " + outputFile);
	}

	private static void getData(String[] args, int detectorsPerSegment,
			int numberOfSegments, int hours, String outputFile) {
		if (args.length == 1 && args[0].equals("-help")) {
			usage();
			return;
		}

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-segments")) {
				if (i + 1 >= args.length) {
					System.out
							.println("Please provide an argument for segments.");
					return;
				} else {
					try {
						numberOfSegments = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.out.println("Invalid argument to -segments");
						return;
					}
				}
				i++; // cover argument
			} else if (args[i].equals("-detectors")) {
				if (i + 1 >= args.length) {
					System.out
							.println("Please provide an argument for detectors.");
					return;
				} else {
					try {
						detectorsPerSegment = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.out.println("Invalid argument to -detectors");
						return;
					}
				}
				i++; // cover argument
			} else if (args[i].equals("-hours")) {
				if (i + 1 >= args.length) {
					System.out.println("Please provide an argument for hours.");
					return;
				} else {
					try {
						hours = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.out.println("Invalid argument to -hours");
						return;
					}
				}
				i++; // cover argument
			} else if (args[i].equals("-filename")) {
				if (i + 1 >= args.length) {
					System.out
							.println("Please provide an argument for detectors.");
					return;
				} else {
					outputFile = args[i + 1];
				}
				i++; // cover argument
			} else {
				System.out.println("Invalid flag.");
				usage();
				return;
			}
		}
	}


	
	private static void usage() {
		System.out.println("");
		System.out
				.println("Usage: java niagara.misc.trafficDataGenerator [flags]");
		System.out.println("\t-segments <number>     Number of map segments");
		System.out
				.println("\t-detectors <number>    Number of detectors per segment");
		System.out.println("\t-filename <filename>   Name of output file");
		System.out.println("\t-hours <number>        Hours in simulation");
		System.out.println("\t-help                  Print this help screen");
		System.out
				.println("\nDefault values: 5 detectors per segment, 9 segments, 4 hours, filename \"stream.xml\"");
	}

}
