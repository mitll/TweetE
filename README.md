# TweetE
Tools for scraping of twitter data, conversion, text analysis and graph construction. This software consists of several easy-to-use Python modules for several aspects of natural language processing with Twitter

### twitter_analysis
The MITLL TweetE Twitter Analysis Tools perform multiple types of analysis on Twitter data:
  * Unstructured tweets to structured data and text normalization
  * Twitter graph creation
Tweets are ingested from a flat TSV (tab-separated value) file. Results are stored in a serialized Python object (text analysis and normalization) and multiple graph formats.  Examples of research applications that used these tools are contained in the papers [WCampbell13] and [WCampbell14].

Provided a collection of tweets, the MITLL TweetE Twitter Analysis Tools:
* 1.	Normalize the input text and remove links and non-language characters
* 2.	Extract information: hashtags, links, at-mentions
* 3.	Filter out all documents not matching the user-specified language
* 4.	Filter by geo location
* 5.	Store the results in serialized files for graph creation, analysis with the MIT topic tools, or other counts-based classifiers
* 6.	Create rich Twitter graphs 
These tools are command-line applications mainly suited for researchers who would like to convert Twitter data into structured form for further high-level analysis—e.g., natural language processing and graph analysis.


### pyTweet
This module enables data scientists to build large datasets for graph analytics. It can be very difficult to obtain data sets for big data analysis, along with developing architecture for processing and storage. With pyTweet, a user can easily select a sampling method and have the collection run unsupervised. Profile and timeline metadata are saved in JSON file format. Modules add-ons can process the JSON files into a PostgreSQL database with a graph-like schema. See the README.pdf and README.md within pyTweet fro getting started.

