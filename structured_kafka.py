import praw
from kafka import KafkaProducer
import json

reddit = praw.Reddit(client_id='',
                     client_secret='',
                     user_agent='bda')

# Kafka configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Subreddit to stream comments from
subreddit = reddit.subreddit('movies')

for comment in subreddit.stream.comments():
    try:
        message = {'author': str(comment.author), 'body': comment.body}
        producer.send('topic1', value=message)
        print(f"Sent: {message}")
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")
