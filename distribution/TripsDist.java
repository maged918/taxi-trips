package distribution;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class TripsDist {
	
	public static class DistMapper extends Mapper<Object, Text, Text, IntWritable>{
		
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
		
		public static double toRadian(double degree){
			return degree * 3.14 / 180;
		}

		public static double computeDistance(double lat1, double long1, double lat2, double long2){
			double R =  6371.009;
			double deltaLat = lat1 - lat2;
			double deltaLong = long1 - long2;
			double meanLat = (lat1 + lat2)/2;
			return R * Math.sqrt(Math.pow(deltaLat, 2) + Math.pow(Math.cos(meanLat) * deltaLong, 2));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			TimeZone tz = TimeZone.getTimeZone("America/San_Fransisco");
			
			StringTokenizer st = new StringTokenizer(value.toString());
			st.nextToken(); 
			
			long startTime = ((long) Double.parseDouble(st.nextToken())) * 1000L;
		    Calendar cal1 = Calendar.getInstance(tz);
		    cal1.setTimeInMillis(startTime);
			
		    double lat1 = toRadian(Double.parseDouble(st.nextToken()));
			double long1 = toRadian(Double.parseDouble(st.nextToken()));
			
			long endTime = ((long) Double.parseDouble(st.nextToken())) * 1000L;
			Calendar cal2 = Calendar.getInstance(tz);
			cal2.setTimeInMillis(endTime);
			
			long secondsBetween = Math.abs(TimeUnit.MILLISECONDS.toSeconds(cal1.getTime().getTime() - cal2.getTime().getTime()));
			if(secondsBetween == 0) return;
			double hours = secondsBetween/3600.0;
			
			double lat2 = toRadian(Double.parseDouble(st.nextToken()));
			double long2 = toRadian(Double.parseDouble(st.nextToken()));
			if(lat1 == lat2 && long1 == long2) return;
			
			double distance = computeDistance(lat1, long1, lat2, long2);
			double speed = distance/hours;
			if(speed >200) return;
			
			word.set(((int)distance) + "");
			context.write(word, one);
			
		}
	}
	
	public static class DistReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
		InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[]args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "trip dist");
		job.setJarByClass(TripsDist.class);
		job.setMapperClass(DistMapper.class);
		job.setReducerClass(DistReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    FileInputFormat.setMaxInputSplitSize(job, (int)Math.pow(10, 6));
	    job.setNumReduceTasks(10);
	    
	    //System.exit(job.waitForCompletion(true) ? 0 : 1);
	    long start = new Date().getTime();
	    boolean status = job.waitForCompletion(true);            
	    long end = new Date().getTime();
	    
	    System.out.println("Job took "+ TimeUnit.MILLISECONDS.toMillis(end - start) + " milliseconds");
	}
}
