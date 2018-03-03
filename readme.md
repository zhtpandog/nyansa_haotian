# Q1 #
## Execution ##
With Python 2 or 3 environment configured in your device, cd to folder ex1, and make sure both ex1.py and input file exists, then run command:  
`python ex1.py input.txt`  
The result will look like this:  
```
08/08/2014 GMT
www.facebook.com 2
www.google.com 2
news.ycombinator.com 1
08/09/2014 GMT
www.nba.com 3
sports.yahoo.com 2
www.cnn.com 1
08/10/2014 GMT
www.twitter.com 1

```  
## Analysis ##
### Time Complexity ###
Suppose input file contains `N` lines of record, and for each date, there are on average `M` distinct URLs, and there are `K` distinct dates.  
1) Iterate through each line and create dictionary: O(N)  
2) Sort URL within each date by frequency: O(MlogM)  
3) Sort dates: O(KlogK)  
Total: O(N + MlogM + KlogK)  

### Space Complexity ###
Space cost somes from storing records into a dictionary. In worst case (each visit within each day is distinct), it is O(N).  

## Improvements ##
### URL lookup ###
Suppose an output looks like this:  
```
08/08/2014 GMT
www.facebook.com 2
08/09/2014 GMT
www.facebook.com 3
08/10/2014 GMT
www.facebook.com 1

``` 
According to the method previously described, the facebook URL will be stored three times (within each date). So when the system scales up, lots of space would be wasted due to such kind of duplication.  
We can create a URL lookup table (dictionary) with keys as URL string and values as integer URL ID, with URL ID auto incrementing when seeing a new URL. So we only need to store integer URL ID within our running dictionary.  

### Streaming data time windowing ###
In a real-world system, the input records could be partially sorted, which means records are clustered within a short time window. For example, records within a week are not sorted, but records from different weeks are bot blended with each other. If this is the case, we do not need to cache all the data, or sort dates from MIN_DATE to MAX_DATE in record. We can segment data by time window (e.g. week), and process data within each segment, with data from other segments persisting in disk.  

### Scaling up ###
When the number of records become large, we may adopt Map Reduce idea to achieve parallized processing. Here are the steps:  
1) Parallelize the records into multiple partitions.  
2) Map each record into format ((date, url), 1)), in which different combinations of dates and urls are used as keys.  
3) Group by key and get the counts (e.g. ((2014/08/08, www.facebook.com), 20000), URL lookup discussed above can also be used.)











