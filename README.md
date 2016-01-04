# Bingle-Search-Engine

Overview: Web Search Engine containing web crawler, TF-IDF indexer, PageRank generator, web interface and database. This project was completed by Ryan Smith, Josh Kessler, Josh Fried and Max Tromanhauser. Demonstration video available on YouTube here: https://youtu.be/MAX6daUNPCY.

Use: This project provides a full web search engine with results ranked using a balance of TF/IDF and PageRank scores. It includes a distributed web crawler which saves documents to S3 buckets and their information to DynamoDB where it can be accessed by PageRank and Indexer components. PageRank and Indexer run as Map Reduce jobs to process the data and populate their own DynamoDB tables with data linking words to document scores and URLs. A robust web interface built on the Spring MVC framework queries these databases in response to search phrases and returns the results considered most relevant.


For full details, please see included documentation file.