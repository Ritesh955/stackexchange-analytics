#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql import functions
from pyspark.sql.types import *
import re
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
from collections import defaultdict
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression, RidgeClassifier


# In[60]:


REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
STOPWORDS = set(stopwords.words('english'))


# In[61]:


def text_prepare(text):
    text = text.lower()# lowercase text
    text = REPLACE_BY_SPACE_RE.sub(' ',text)# replace REPLACE_BY_SPACE_RE symbols by space in text
    text = BAD_SYMBOLS_RE.sub('',text)# delete symbols which are in BAD_SYMBOLS_RE from text
    text = ' '.join(word for word in text.split() if word not in STOPWORDS)# delete stopwords from text
    return text


# In[62]:


def cleanhtml(raw_html):    
    a = "".join(raw_html)
    b = a.replace('<',"")
    c = b.replace('>'," ")
    d = c.split()
    return d


# In[63]:


def tfidf_features(X_train, X_val):
    tfidf_vectorizer = TfidfVectorizer(min_df=5, max_df=0.9, ngram_range=(1, 2),token_pattern='(\S+)')
    X_train = tfidf_vectorizer.fit_transform(X_train)
    X_val = tfidf_vectorizer.transform(X_val)
    return X_train, X_val, tfidf_vectorizer.vocabulary_


# In[66]:


def main(inputs):
    posts = spark.read.parquet("/user/cmandava/Users_parquet/Posts_"+inputs+".parquet")
    posts = posts.select('Id','Title','Tags','sites')
    posts = posts.filter(posts['Title'].isNotNull())
    df = posts.toPandas()
    df['Title'] = df['Title'].apply(lambda x:text_prepare(x))
    df['Tags'] = df['Tags'].apply(lambda x:cleanhtml(x))
    data = df.iloc[:,1:3]
    train, validation = train_test_split(data, test_size=0.2)
    X_train, y_train = train['Title'].values, train['Tags'].values
    X_val, y_val = validation['Title'].values, validation['Tags'].values
    
    tags_counts = defaultdict(int)
    words_counts = defaultdict(int)
    for tags in y_train:
        for tag in tags:
            tags_counts[tag] += 1
    for text in X_train:
        for word in text.split():
            words_counts[word] += 1
    
    most_common_tags = sorted(tags_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    most_common_words = sorted(words_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    print("This are the 3 most common tags for "+inputs+ " in stack overflow",most_common_tags)
    
    print("This are the 3 most common words for "+inputs+ " in stack overflow",most_common_words)

# In[67]:


if __name__ == "__main__":
    spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
    inputs = sys.argv[1]
    main(inputs)


# In[ ]:

#How to run this file?
#spark-submit common_tags_words.py datascience

