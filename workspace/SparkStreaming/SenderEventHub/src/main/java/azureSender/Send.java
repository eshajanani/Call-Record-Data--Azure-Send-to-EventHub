package azureSender;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.joda.time.DateTime;
import azureSender.DataGenerator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

public class Send
{

	public static void main(String[] args) 
			throws ServiceBusException, ExecutionException, InterruptedException, IOException
	{
		
	        final String namespaceName = "ds-bi-dw-poc-ns";
	        final String eventHubName = "dsg_hdinsight_webinar";
	        final String sasKeyName = "webinar";
	        final String sasKey = "vElX0bi7cdMC1WMd/nohLNoe7OfTWP8RWhK1Dl6PXGk=";
	        
	        String connectionString = "Endpoint=sb://ds-bi-dw-poc-ns.servicebus.windows.net/;SharedAccessKeyName=webinar;SharedAccessKey=vElX0bi7cdMC1WMd/nohLNoe7OfTWP8RWhK1Dl6PXGk=;EntityPath=dsg_hdinsight_webinar";
	        ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(connectionString);

		Gson gson = new GsonBuilder().create();
		EventHubClient sender = EventHubClient.createFromConnectionString(eventHubConnectionString.toString()).get();
		
		while (true)
		{
			Random rn = new Random();
			LinkedList<EventData> events = new LinkedList<EventData>();
			for ( int i =0; i < 10; i++){
				
				//random.nextInt(max - min + 1) + min
				int ran_num =rn.nextInt(40 - 1 + 1) + 1;
				//System.out.println("Random number:" +ran_num);
				PayloadEvent payload = new PayloadEvent(ran_num, rn);
				System.out.println("Data sent:" +payload.customer_id_from +","+payload.towerID+","+payload.tower_band+" ,"+payload.tower_freq+","+payload.call_quality+","+payload.call_type);
				byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				EventData sendEvent = new EventData(payloadBytes);
				Map<String, String> applicationProperties = new HashMap<String, String>();
				applicationProperties.put("from", "javaClient");
				sendEvent.setProperties(applicationProperties);
				events.add(sendEvent);
			}

			sender.send(events).get();
			System.out.println(String.format("Sent Batch... Size: %s ", events.size()));
		//	System.out.println("Also " + sender.send(events).toString());
		}		
	}

	/**
	 * actual application-payload, ex: a CDR event
	 */
	static final class PayloadEvent
	{
		PayloadEvent(final int seed, Random rn)
		{
			DataGenerator dg = new DataGenerator();
			this.towerID = "t"+seed;
			String[] tower_details = dg.generate_tower_freq();
			this.tower_freq = Integer.parseInt(tower_details[0]);
			this.tower_band = tower_details[1];
			this.customer_id_from = dg.generate_from_cust(seed);
			this.call_type = dg.generate_call_type(seed);
			if( this.call_type == "Data")
			{
				this.customer_id_to = "N/A";
				
			}
			else
			{
				this.customer_id_to = dg.generate_to_cust(seed);
			}
			
			long t1 = System.currentTimeMillis() + rn.nextInt();
			   long t2 = t1 + 2 * 60 * 1000 + rn.nextInt(60 * 1000) + 1;
			    this.d1 = new DateTime(t1).toString();
			    
			    this.d2 = new DateTime(t2).toString();
			    if ( this.call_type == "sms")
			    {
			    	this.d2 = new DateTime(t1).toString();	
			    }
			    this.call_result = dg.get_call_result(seed, this.call_type);
			    
			   if( this.call_result == "BUSY")
			   {
				   this.d2 = new DateTime(t1).toString();	
			   }
			 //  this.month = new DateTime(t1).getMonthOfYear();
			   this.charge = rn.nextFloat();
			   this.call_quality = dg.get_call_quality(this.call_result,seed, this.call_type);
			   this.call_protocol = dg.get_call_protocol(this.call_type);
			   this.data_speed = dg.get_data_protocol(this.call_type);
			   if( this.call_type == "Data"){
					this.data_usage =rn.nextFloat();
					
				}	
				else
				{
					this.data_usage = (float) 0.0;
					
				} 
			   this.weather = dg.get_weather(this.call_quality);
			   
			   
		}
		
		public String towerID;
		public int tower_freq;
		public String tower_band;
		public String customer_id_from;
		public String call_type;
		public String customer_id_to;
	//	public long t1;
	//	public long t2;
		public String  d1;
		public String d2;
	//	public int month;
		public String call_result;
		public Float charge;
		public String call_quality;
		public String call_protocol;
		public String data_speed;
		public Float data_usage; 
		public String weather;
		
	}
}