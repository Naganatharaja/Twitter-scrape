import pandas as pd
import json
import pymongo
import snscrape.modules.twitter as sntwitter
import streamlit as st



st.header('Twitter Scrape')
hashtag=st.text_input("Enter Your Hashtag: ")
startDate = st.date_input("Enter starting date: ")
Enddate =st.date_input("Enter End date: ")
Nooftweet= st.number_input("Enter the No of tweets you want: ")
#scrape Twitter data
def scrapeTwitterData(hashtag, startDate,Enddate,Nooftweet):
    scraper = sntwitter.TwitterSearchScraper(
        f"#{hashtag} since:{startDate} until:{Enddate}")
    tweets = []

    for i,tweet in enumerate(scraper.get_items()):
        data = [tweet.date,tweet.id,tweet.url,tweet.content,tweet.user.username,tweet.replyCount,
                tweet.retweetCount,tweet.lang,tweet.source,tweet.likeCount]
        tweets.append(data)
        if i>= Nooftweet-1:
            break
    tweet_df=pd.DataFrame(tweets, columns=["DATE","ID",'URL','CONTENT','USERNAME','REPLYCOUNT','RETWEETCOUNT','LANGUAGE',
                                            'SOURCE','LIKECOUNT'])
    return tweet_df

#share scrapped data to mongodb
def uploadDataToMDB(tweet_df):
    dataToMDB = tweet_df.to_dict('records')


    connection = pymongo.MongoClient("mongodb://Aaj:****@ac-efdo7na-shard-00-00.5jlca6i.mongodb.net:27017,ac-efdo7na-shard-00-01.5jlca6i.mongodb.net:27017,ac-efdo7na-shard-00-02.5jlca6i.mongodb.net:27017/?ssl=true&replicaSet=atlas-r7j66y-shard-0&authSource=admin&retryWrites=true&w=majority")

    db = connection['Twitter']
    coll = db['scrapped_tweet']
    coll.insert_many(dataToMDB)
#buttons

enter_btn = st.button('Enter')
if st.session_state.get('button') !=True:
    st.session_state['button']=enter_btn
if st.session_state['button']==True:
    tweet_df= scrapeTwitterData(hashtag,startDate,Enddate,Nooftweet)
    st.dataframe(tweet_df)

    col1,col2,col3 =st.columns([1,1,1])
    with col1:
      if st.button('Upload to Mongodb'):
         uploadDataToMDB(tweet_df)
         st.write('YOU HAVE SUCCESSFULLY UPLOADED THE SCRAPED DATA TO MONGODB')

    def convert_csv(df):
        return df.to_csv(index=False).encode('utf-8')
    csv = convert_csv(tweet_df)
    with col2:
         st.download_button(
                     label='Download as CSV',
                     data=csv,
                     file_name='Twitter_data.csv',
                     mime="text/csv",
         )

    jsonFile = tweet_df.to_json(orient="records")
    with col3:

         st.download_button(
                    label='Download as Json',
                     data=jsonFile,
                     file_name='Twitter_json.json',
                     mime="text/json",
         )
