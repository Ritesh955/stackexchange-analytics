#!/usr/bin/env python
# coding: utf-8

# In[59]:


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


# In[64]:


def train_classifier(X_train, y_train):
    clf = OneVsRestClassifier(RidgeClassifier(normalize=True))
    clf.fit(X_train, y_train)
    return clf


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
    
    X_train_tfidf, X_val_tfidf,tfidf_vocab = tfidf_features(X_train, X_val)
    tfidf_reversed_vocab = {i:word for word,i in tfidf_vocab.items()}
    
    mlb = MultiLabelBinarizer(classes = sorted(tags_counts.keys()))
    y_train = mlb.fit_transform(y_train)
    
    classifier = train_classifier(X_train_tfidf, y_train)
    
    y_val_predicted_labels_tfidf = classifier.predict(X_val_tfidf)
    y_val_predicted_scores_tfidf = classifier.decision_function(X_val_tfidf)
    
    y_val_pred_inversed = mlb.inverse_transform(y_val_predicted_labels_tfidf)
    for i in range(3):
        print('Title:\t{}\nTrue labels:\t{}\nPredicted labels:\t{}\n\n'.format(
            X_val[i],
            ','.join(y_val[i]),
            ','.join(y_val_pred_inversed[i])
        ))


# In[67]:


if __name__ == "__main__":
    spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
    inputs = sys.argv[1]
    main(inputs)


# In[ ]:

#How to run this file?
#spark-submit tag_predictor.py datascience


