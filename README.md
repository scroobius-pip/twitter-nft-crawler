
# twitter-nft-crawler
This project uses the twitter api to gather millions of twitter user profile pictures storing them  on a redis instance in a [redis set](https://redis.io/commands/sadd). It recursively crawls starting from an initial user profile, getting their followers, their followers of followers, ad infinitum, till manually stopped

It uses [bullmq](https://docs.bullmq.io) as an internal task queue for all imports (getting twitter user info) and exports (storing in a redis instance)
