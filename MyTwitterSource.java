
package flume.mytwittersource;


import java.util.*;

import org.apache.flume.*;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.*;


public class MyTwitterSource {

	private String consumerKey;
	private String consumerSecret;
	private String accessToken;
	private String accessTokenSecret;

	private String keywords;
	
	Twitter mytwitter = null;
	
	MyTwitterSource(Context context)
	{
		initialize(context);
	}
	
	public void initialize(Context context) {
	
		consumerKey = context.getString("consumerKey");
		consumerSecret = context.getString("consumerSecret");
		accessToken = context.getString("accessToken");
		accessTokenSecret = context.getString("accessTokenSecret");
	
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);
		cb.setJSONStoreEnabled(true);
		//cb.setIncludeEntitiesEnabled(true);

		String keywordString = context.getString("keywords", "");
		keywords = keywordString.replace(",", "OR");

		mytwitter = new TwitterFactory(cb.build()).getInstance();
	}
	
	List<String> fetchTweets()
	{
		List<Status> allTweets = null;
		
		List<String> tweets = new ArrayList<String>();

					
		try {
            		Query query = new Query(keywords);
			QueryResult result;
			
			do {
				
				result = mytwitter.search(query);
				
				allTweets = result.getTweets();

				for (Status tweet: allTweets)
				{
					tweets.add(tweet.getText());
				}

			} while ((query = result.nextQuery()) != null);
		} 
		catch (TwitterException te) {
            		te.printStackTrace();
            		System.out.println("Failed to search tweets: " + te.getMessage());
            		System.exit(-1);
        	}
        	catch (Exception e) {
            		e.printStackTrace();
        	}
		
		return tweets;
	}
	
};
