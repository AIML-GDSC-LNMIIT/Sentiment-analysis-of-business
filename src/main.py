from src.exception import CustomException
from src.logger import logging
from src.data_collection import DataCollection
import string
import sys
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('vader_lexicon')

if __name__ == "__main__" :
    obj = DataCollection()
    raw_data_path = obj.initiate_data_collection()
    try :
        df = pd.read_csv(raw_data_path)
    except Exception as e :
        logging.info("Exception Occured at reading the raw data")
        raise CustomException(e,sys)

    df['business'] = df['business'].apply(lambda x : x.replace("/", ""))

    try :
        for idx, row in df.iterrows() :
            text = row['review']
            lower_case = text.lower()
            cleaned_text = lower_case.translate(str.maketrans('', '', string.punctuation))
            score = SentimentIntensityAnalyzer().polarity_scores(cleaned_text)
            df.at[idx, 'Sentiment_Score_neg'] = score['neg']
            df.at[idx, 'Sentiment_Score_pos'] = score['pos']
            if score['neg'] > score['pos']:
                df.at[idx, 'Sentiment_Score_remark'] = "Negative Sentiment"
            elif score['neg'] < score['pos']:
                df.at[idx, 'Sentiment_Score_remark'] = "Positive Sentiment"
            else:
                df.at[idx, 'Sentiment_Score_remark'] = "Neutral Sentiment"
    except Exception as e :
        logging.info("Exception Occured at determining sentiment score and remark")
        raise CustomException(e,sys)

    df.drop(inplace=True, columns=['review'])
    try : 
        df.to_csv('/config/workspace/artifacts/SentimentAnalysisOfBusinesses.csv', index=False)
    except Exception as e :
        logging.info("Exception Occured at generating final file")
        raise CustomException(e,sys)