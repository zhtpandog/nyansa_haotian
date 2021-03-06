# Updates #
## Q1 Improvemented Version ##
### Background ###
Cardinality (i.e. number of distinct values) of hit count values and the number of days are much smaller than the number of unique URLs.  
So we should try to avoid sort URLs. It can be achieved using Bucket Sort.  
### Process ###
First, going through records line by line, and compose dictionary with structure {date: {URL: frequency}}. e.g.  
```
{'20140808': {'news.ycombinator.com': 1, 'www.facebook.com': 2, 'www.google.com': 2},
 '20140809': {'sports.yahoo.com': 2, 'www.cnn.com': 1, 'www.nba.com': 3},
 '20140810': {'www.twitter.com': 1}}
```
During the first pass, we also maintaining a variable recording maximum URL hits per day per URL. In the above example, it is 3.  
Meanwhile, we initiate a placeholder dictionary for next step looks like this:  
```
{'20140808': [],
 '20140809': [],
 '20140810': []
```
  
Then, we fill the empty lists in placeholder dictionary created in the previous step. The size of each list is the maximum URL hits per day per URL + 1. The __index__ is the hit frequency for the URLs fall into that slot. Since the index is ordered, we do not need to sort URLs. The outcome looks like this:  
```
{'20140808': [[], ['news.ycombinator.com'], ['www.facebook.com', 'www.google.com'], []],
 '20140809': [[], ['www.cnn.com'], ['sports.yahoo.com'], ['www.nba.com']],
 '20140810': [[], ['www.twitter.com'], [], []]}
```
  
Finally, we sort the result by dates (dictionary key), and print URLs and hits from end to beginning:  
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
### Time Complexity ###
Suppose input file contains `N` lines of record, and for each date, there are on average `M` distinct URLs, and there are `K` distinct dates.  
1) Iterate through each line and create dictionary: `O(N)`  
2) Put URL in correct indices based on frequency: `O(M)`   
3) Sort dates: `O(KlogK)`  
Total: `O(N + M + KlogK)`  

### Space Complexity ###
Space cost comes from storing records into a dictionary. In worst case (each visit within each day is distinct), it is `O(N)`.  

### Tiny Further Improvements ###
The dates can also be sorted using bucket sort by placing dates in sequential indices. This can slightly improve time complexity into `O(N + M + K)`. But since the assumption says there are much fewerer dates than URLs, this improvement is trivial.  
Also, for space, the URL lookup idea I discussed before can also be applied.  

### Conclusion ###
This method takes advantage of the assumption and achieving performance boost by not sorting URLs.  

# Summary #
Q1 is completed using Python. Complexity analysis and improvements for conditions like big data, partially sorted data source, and MapReduce scaling up are dicussed.  
Q2 is completed using both Python (native `map`&`reduce` function) and Spark in two separate files. All codes are runnable and tested. Detailed process walk-through is offered in readme.  

# Q1 #
## Execution ##
With Python 2 or 3 environment configured in your device, `cd` to folder `ex1`, and make sure both `ex1.py` and `input.txt` exist, then run command:  
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
1) Iterate through each line and create dictionary: `O(N)`  
2) Sort URL within each date by frequency: `O(MlogM)`    
3) Sort dates: `O(KlogK)`  
Total: `O(N + MlogM + KlogK)`  

### Space Complexity ###
Space cost comes from storing records into a dictionary. In worst case (each visit within each day is distinct), it is `O(N)`.  

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

### Time windowing ###
In a real-world system, the input records could be partially sorted, which means records are clustered within a short time window. For example, records within a week are not sorted, but records from different weeks are bot blended with each other. If this is the case, we do not need to cache all the data, or sort dates from MIN_DATE to MAX_DATE in record. We can segment data by time window (e.g. week), and process data within each segment, with data from other segments persisting in disk.  

### Scaling up ###
When the number of records become large, we may adopt Map Reduce idea to achieve parallized processing. Here are the steps:  
1) Parallelize the records into multiple partitions.  
2) Map each record into format ((date, url), 1)), in which different combinations of dates and urls are used as keys.  
3) Group by key and get the counts (e.g. ((2014/08/08, www.facebook.com), 20000), URL lookup discussed above can also be used.)

# Q2 Python MapReduce #
## Execution ##
With Python 2 or 3 environment configured in your device, `cd` to folder `ex2`, and make sure both `ex2.py` and `input.txt` exist, then run command:  
`python ex2.py input.txt`  
The result will look like this:  
```
file loaded
phase 1 map finished
phase 1 reduce finished
phase 2 map finished
phase 2 reduce finished
phase 3 map finished
result: 
iphone
```

## Example walk through ##
Given input:  
```
X1 = {"1.1.1.1", "android", 20}
X2 = {"1.1.1.1", "android", 100}
X3 = {"2.2.2.2", "iphone", 10}
X4 = {"2.2.2.2", "iphone", 20}
X5 = {"3.3.3.3", "android", 10}
X6 = {"3.3.3.3", "android", 40}
X7 = {"3.3.3.3", "android", 10}
```
### Phase 1 Map ###
Process the input and generate following maps:  
```
[('1.1.1.1', 'android', 20, 1),
 ('1.1.1.1', 'android', 100, 1),
 ('2.2.2.2', 'iphone', 10, 1),
 ('2.2.2.2', 'iphone', 20, 1),
 ('3.3.3.3', 'android', 10, 1),
 ('3.3.3.3', 'android', 40, 1),
 ('3.3.3.3', 'android', 10, 1),
 ('4.4.4.4', 'iphone', 10, 1)]
```
### Phase 1 Shuffle ###
After grouping by id:  
```
{'1.1.1.1': [['android', 20, 1], ['android', 100, 1]],
 '2.2.2.2': [['iphone', 10, 1], ['iphone', 20, 1]],
 '3.3.3.3': [['android', 10, 1], ['android', 40, 1], ['android', 10, 1]],
 '4.4.4.4': [['iphone', 10, 1]]}
```
### Phase 1 Reduce ###
Calculate the sum and count for each id:  
```
{'1.1.1.1': ('android', 120, 2),
 '2.2.2.2': ('iphone', 30, 2),
 '3.3.3.3': ('android', 60, 3),
 '4.4.4.4': ['iphone', 10, 1]}
```
### Phase 2 Map ###
If sum / count <= 50, then poor = True. Generate following maps:
```
[('android', False, 0, 0),
 ('iphone', True, 0, 0),
 ('android', True, 0, 0),
 ('iphone', True, 0, 0)]
```
### Phase 2 Shuffle ###
After group by device_type:
```
{'android': [[False, 0, 0], [True, 0, 0]],
 'iphone': [[True, 0, 0], [True, 0, 0]]}
```
### Phase 2 Reduce ###
For each device_type, calculate total number of devices and poor devices:  
```
{'android': (True, 2, 1), 'iphone': (True, 2, 2)}
```
### Phase 3 Map ###
Calculate poor-ratio for each device_type:  
```
[('android', 0.5), ('iphone', 1.0)]
```
### Produce Result ###
Sort by poor ratio from high to low. Output device(s) with highest poor-ratio:  
```
iphone
```

# Q2 Spark MapReduce #
## Execution ##
The code is tested on Spark version 2.1.0, with Python 2.7.  
cd to folder `ex2`, and make sure both `ex2_spark.py` and `input.txt exist`, then run command:  
`spark-submit ex2_spark.py input.txt`  
The result will look like this:  
`iphone`  

## Example walk through ##
Given input:  
```
X1 = {"1.1.1.1", "android", 20}
X2 = {"1.1.1.1", "android", 100}
X3 = {"2.2.2.2", "iphone", 10}
X4 = {"2.2.2.2", "iphone", 20}
X5 = {"3.3.3.3", "android", 10}
X6 = {"3.3.3.3", "android", 40}
X7 = {"3.3.3.3", "android", 10}
```
### Phase 1 Map ###
Process the input and generate following maps:  
```
[(u'1.1.1.1', u'android', 20), 
(u'1.1.1.1', u'android', 100), 
(u'2.2.2.2', u'iphone', 10), 
(u'2.2.2.2', u'iphone', 20), 
(u'3.3.3.3', u'android', 10), 
(u'3.3.3.3', u'android', 40), 
(u'3.3.3.3', u'android', 10), 
(u'4.4.4.4', u'iphone', 10)]
```
### Phase 1 aggregateByKey ###
Calculate the sum and count for each id:  
```
[(u'3.3.3.3', (60, 3, u'android')), 
(u'4.4.4.4', (10, 1, u'iphone')), 
(u'1.1.1.1', (120, 2, u'android')), 
(u'2.2.2.2', (30, 2, u'iphone'))]
```
### Phase 2 Map ###
If sum / count <= 50, then poor = True. Generate following maps:
```
[(u'android', True), 
(u'iphone', True), 
(u'android', False), 
(u'iphone', True)]
```
### Phase 2 aggregateByKey ###
For each device_type, calculate total number of devices and poor devices:  
```
[(u'android', (2, 1)), 
(u'iphone', (2, 2))]
```
### Phase 3 Map ###
Calculate poor-ratio for each device_type:  
```
[(u'android', 0.5), 
(u'iphone', 1.0)]
```
### Produce Result ###
Sort by poor ratio from high to low. Output device(s) with highest poor-ratio:  
```
iphone
```














