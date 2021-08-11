package flume.mytwittersource;

import java.util.*;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.*;

import twitter4j.conf.*;
import twitter4j.*;

public class MyTwitterSourceForFlume extends AbstractSource
    implements EventDrivenSource, Configurable {

	MyTwitterSource myTwitterSrc = null;
	
	public void configure(Context context) {
		myTwitterSrc = new MyTwitterSource(context); 
	}
	
	public void start() {
		// The channel is the piece of Flume that sits between the Source and Sink,
		// and is used to process events.
		final ChannelProcessor channel = getChannelProcessor();
	
	
		List<String> strAllTweets = myTwitterSrc.fetchTweets();

		 
	
		//Event event = EventBuilder.withBody( DataObjectFactory.getRawJSON(allTweets).getBytes());
		
		for(String tweet: strAllTweets)
		{	
			Event event = EventBuilder.withBody( tweet.getBytes());

			channel.processEvent(event);
		}
	
	}
	
	public void stop() {
		
		super.stop();
	}
	
};
