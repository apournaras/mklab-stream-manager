socialsensor-stream-manager
===========================

<h1>Stream Manager</h1>

<h3>Stream Manager monitors a set of seven social streams : Twitter, Facebook, Instagram, Google+, Flickr, Tumblr and Youtube to collect incoming content relevant to a keyword, a user or a location, using the corresponding API that is supported from each service. The framework also provides working APIs to store the retrieved items in different storages.</h3>

<h2><u>Getting started</u></h2>
<p>In order to run StreamManager,one only has to set the configuration file <i>'conf/streams.conf.xml'</i>. In the configuration file are defined all the required parameters for the storage and stream set up. The project supports five different types of storages that the user can employ to store collected content by defining the ip address, the name of the database, the collection where the retrieved items are going to be stored or the path that the file will be located. The supported storages are: MongoDB, Solr, Lucene, Redis and Flatfile. For the streams' configuration the user has to add the necessary keys and tokens that each social network api provides to the developer. Also, other fields that need to be determined are the request rate by which the monitor is going to perform the api calls (<i>requestPeriod</i>) and the oldest publication date of the items to be retrieved (<i>since</i>). For the retrieval process, one of the following fields is obligatory to be defined for the Facebook, Instagram, Google+, Flickr, Tumblr and Youtube streams: the keywords that are going to be used for the search (<i>keywords</i>), the social network user whose public uploaded content will be followed (<i>follows</i>), the location that the collected content is related to (<i>locations</i>). In contrast to the other six streams, Twitter can also function without defining the latter and collect incoming content of no specific origin.</p>

<p>When the configuration file is set up, the user can run the StreamManager class located in the following path : <i>'src/main/java/eu/socialsensor/sfc/ streams/management'</i> and collect relevant content from the selected social networks. One important thing to note is that even though the retrieval processes from Facebook, Instagram, Google+, Flickr, Tumblr and Youtube streams can run simultaneously at the same execution, this cannot apply to the Twitter stream as well. This is due to the fact that the Twitter API acts as a subscriber to a channel, whereas the other six behave as polling consumers. </p>

<h2><u>Learning more</u></h2>
<p><h4>Storage Documentation</h4></p>

<ul>
<li>More information regading MongoDB : <a href="http://www.mongodb.org/">MongoDB</a></li>
<li>More information regading Solr : <a href="http://lucene.apache.org/solr/">Solr</a></li>
<li>More information regading Lucene : <a href="http://lucene.apache.org/">Lucene</a></li>
<li>More information regading Redis : <a href="http://redis.io/">Redis</a></li>
<li>More information regading FlatFile : <a href="http://en.wikipedia.org/wiki/Flat_file_database">FlatFile</a></li>
</ul>

<p><h4>Stream API Documentation</h4></p>

<ul>
<li>More information regading Twitter API : <a href="http://twitter4j.org/en/">Twitter API</a></li>
<li>More information regading Facebook API : <a href="http://restfb.com/">Facebook API</a></li>
<li>More information regading Instagram API : <a href="https://github.com/sachin-handiekar/jInstagram">Instagram API</a></li>
<li>More information regading Google+ API: <a href="https://developers.google.com/+/quickstart/java">Google+ API</a></li>
<li>More information regading Flickr API: <a href="http://www.flickr.com/services/api/">Flickr API</a></li>
<li>More information regading Tumblr API: <a href="https://github.com/tumblr/jumblr">Tumblr API</a></li>
<li>More information regading Youtube API: <a href="https://developers.google.com/youtube/v3/">Youtube API</a></li>
</ul>

