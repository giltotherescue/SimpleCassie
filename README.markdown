# SimpleCassie

SimpleCassie is a Cassandra wrapper for PHP. The [official project is on Google Code](http://code.google.com/p/simpletools-php/wiki/SimpleCassie) but this is a fork by Gil Hildebrand.

This fork includes several improvements:

- A `parse()` method to convert a Thrift response to a PHP array.
- Column expiration (TTL) support by [Zhengjun Chen](mailto:zhjchen.sa@gmail.com)
- Removal of Thrift client dependencies (for decent performance, the compiled version should always be used)
- Changed the default number of rows in count() from 100 to 100,000
- Changed the order of arguments on the count() function for backwards compatibility

