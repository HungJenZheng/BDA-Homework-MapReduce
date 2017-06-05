
# coding: utf-8

# #Programming Exercise: MapReduce
# Goal: A MapReduce program for analyzing the check-in records
# Input: Check-in records in social networking site foursquare
# Check-in records: <user_id, venue_id, checkin_time>
# Venues: <venue_id, category, latitude, longitude>
# Output: Detailed analysis including the following tasks:
#     Lists the top checked-in venues (most popular)
#     Lists the top checked-in users
#     Lists the most popular categories
#     Lists the most popular time for check-ins (in time slots in hours, for example, 7:00-8:00 or 18:00-19:00)

# In[51]:


#import findspark
#findspark.init()
import pyspark
from datetime import datetime
import time
from operator import add
sc = pyspark.SparkContext()
import pprint


# Load Files

# In[52]:


checking_local_dedup = sc.textFile("./4sq_data/checking_local_dedup.txt")
local_place = sc.textFile("./4sq_data/local_place.txt")
venue_info = sc.textFile("./4sq_data/venue_info.txt")


# Split String

# In[53]:


def row_split(string):
    items = string.split(',')[1:]
    return (items[0], ' '.join(items[1:]))


# 1. Lists the top checked-in venues (most popular)
#     

# In[54]:


ts = time.time()
checking_place_id = checking_local_dedup     .map(lambda venues : (venues.split(',')[1], 1))     .reduceByKey(add)
place_name = local_place.map(row_split).reduceByKey(lambda x, y: ' '.join([x,y]))
d = checking_place_id.leftOuterJoin(place_name)
result1 = d.sortBy(lambda x: x[1][0], ascending=False)
print('='*80)
print('The top 20 checked-in venues' + ' (time:%.2f)'%(time.time()-ts))
#print('(time:%.2f)'%(time.time()-ts))


# In[55]:


pprint.pprint(result1.take(20))


# 2. Lists the top checked-in users

# In[56]:


ts = time.time()
checking_user_id = checking_local_dedup     .map(lambda entry : (entry.split(',')[0], 1))     .reduceByKey(add)
result2 = checking_user_id.sortBy(lambda x: x[1], ascending=False)
print('='*80)
print('The top 20 checked-in users' + ' (time:%.2f)'%(time.time()-ts))


# In[57]:


pprint.pprint(result2.take(20))


# 3. Lists the most popular categories

# In[58]:


def row_split2(string):
    items = string.split(',')[:-2]
    return (items[0], ' '.join(items[1:]))


# In[59]:


ts = time.time()
result3 = venue_info.map(row_split2)     .join(result1)     .map(lambda item : (item[1][0], item[1][1][0]))     .reduceByKey(add)     .sortBy(lambda item: item[1], ascending=False)

print('='*80)
print('The top 20 popular categories' + ' (time:%.2f)'%(time.time()-ts))


# In[60]:


pprint.pprint(result3.take(20))


# 4. Lists the most popular time for check-ins (in time slots in hours, for example, 7:00-8:00 or 18:00-19:00)

# In[61]:


def convert_time(entry):
    stamp = entry.split(',')[-1]
    hour = int(time.strftime('%H', time.localtime(int(stamp))))
    return ('%2d:00~%2d:00'%(hour, hour+1), 1)


# In[62]:


ts = time.time()
result4 = checking_local_dedup.map(convert_time).reduceByKey(add).sortBy(lambda item: item[1], ascending=False)
print('='*80)
print('The top 20 popular time for check-ins ' + ' (time:%.2f)'%(time.time()-ts))


# In[63]:


pprint.pprint(result4.collect())


# In[ ]:




