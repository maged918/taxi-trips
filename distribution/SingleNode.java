package distribution;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.math.*;
public class SingleNode {
	
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
	
	public static void main (String[] args) throws IOException{
		
		long start = new Date().getTime();
		
		BufferedReader br = new BufferedReader(new FileReader("distribution/2010_03.trips"));
		String trip;
		int count = 0;
		int[] dist = new int[130];
		TimeZone tz = TimeZone.getTimeZone("America/San_Fransisco");

		while((trip=br.readLine())!=null){
			StringTokenizer st = new StringTokenizer(trip);
			st.nextToken(); 
			
			long startTime = ((long) Double.parseDouble(st.nextToken())) * 1000L;
			Calendar cal1 = Calendar.getInstance(tz);
			cal1.setTimeInMillis(startTime);
		    SimpleDateFormat sdf = new SimpleDateFormat(
		            "dd-MMM-yyyy HH:mm:ss.SSS z");    
		    //System.out.println(sdf.format(cal1.getTime()));
		    
		    double lat1 = toRadian(Double.parseDouble(st.nextToken()));
			double long1 = toRadian(Double.parseDouble(st.nextToken()));
			
			long endTime = ((long) Double.parseDouble(st.nextToken())) * 1000L;
			Calendar cal2 = Calendar.getInstance(tz);
			cal2.setTimeInMillis(endTime);
						
			long diff = cal1.getTime().getTime() - cal2.getTime().getTime();
			long seconds = Math.abs(TimeUnit.MILLISECONDS.toSeconds(diff));
			double hours = seconds / 3600.0;
			
			if(seconds == 0) continue;
			
			double lat2 = toRadian(Double.parseDouble(st.nextToken()));
			double long2 = toRadian(Double.parseDouble(st.nextToken()));
			if(lat1 == lat2 && long1 == long2) continue;
			
			double distance = computeDistance(lat1, long1, lat2, long2);
			double speed = distance / hours;
			if(speed > 200) continue;
			
			dist[(int)distance]++;
		}	
		System.out.println(Arrays.toString(dist));
		long end = new Date().getTime();
		System.out.println(TimeUnit.MILLISECONDS.toMillis(end-start));
	}
}
