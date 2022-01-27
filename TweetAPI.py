public interface TweetDatabaseAPI {

    //create tweet
    public long insertTweet(Tweet tweet);

    //create user
    public long insertUser(User user);

    //authenticate
    public void authenticate(String url, String user, String password);

    //close connection
    public void closeConnection();


}
import abc

class TweetAPI(abc.ABC):
    
    @abc.abstractmethod
    def insertTweet(self, tweet):
        pass

    @abc.abstractmethod
    def insertTweet(self, tweet):
        pass

    
