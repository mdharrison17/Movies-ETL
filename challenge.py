import json
import re
import time

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from config import db_password

#create directory path
file_dir = 'C:/Users/03michelleh/Desktop/ClassFolder/Module8/'
#create file name
file_dir = 'C:/Users/03michelleh/Desktop/ClassFolder/Module8/'
#open jason file with directory path and file name variable and load into list of dictionaries
with open(f'{file_dir}wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
#get kaggle and ratings data into df
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv' ,low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')

#the function that does it all
def get_the_data_to_sql(wiki_movies_raw,kaggle_metadata,ratings):
    
    wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]
    
    alt_titles = {}
    
    for movie in wiki_movies:
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
 
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

    #convert wiki movies into df for further data cleansing    
    wiki_movies_df = pd.DataFrame(wiki_movies)
    #extract imbd number only
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    #drop duplicate rows
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    #keep columns with at least 90 non null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    #cleanup box office data
    box_office = wiki_movies_df['Box office'].dropna() 
    
    box_office[box_office.map(lambda x: type(x) != str)]
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    clean_data  = box_office.str.extract(f'({form_one}|{form_two})')
    
    #ASSUMPTION data in a format such as list type throws an error when trying to 
    #clean data with regex andn will stop program from completing.
    for i in range(len(clean_data)):
        s=clean_data.iloc[i,0] 
        try:
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**6
                clean_data.iloc[i,0] = value

            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**9
                clean_data.iloc[i,0] = value

            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                s = re.sub('\$|,','', s)
                value = float(s)
                clean_data.iloc[i,0] = value

        except:
             clean_data.iloc[i,0] = np.NaN  
                
    wiki_movies_df = wiki_movies_df.merge(clean_data, how='outer', left_index=True, right_index=True)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    wiki_movies_df = wiki_movies_df.rename(columns = {0:'Box_office'})
    
    #clean up budget data
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    
    clean_budget  = budget.str.extract(f'({form_one}|{form_two})')

    #ASSUMPTION data in a format such as list type throws an error when trying to 
    #clean data with regex andn will stop program from completing.
    for i in range(len(clean_budget)):
        s=clean_budget.iloc[i,0] 
        try:
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**6
                clean_budget.iloc[i,0] = value

            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
                s = re.sub('\$|\s|[a-zA-Z]','', s)
                value = float(s) * 10**9
                clean_budget.iloc[i,0] = value

            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
                s = re.sub('\$|,','', s)
                value = float(s)
                clean_budget.iloc[i,0] = value

        except:
             clean_budget.iloc[i,0] = np.NaN  
                
        
    wiki_movies_df = wiki_movies_df.merge(clean_budget, how='outer', left_index=True, right_index=True)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    wiki_movies_df = wiki_movies_df.rename(columns = {0:'Budget'})
                
    #parse release date
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    #parse running time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    #clean up the kaggle data

    #ASSUMPTION: incase there is a data conversion that will throw an error
    try:
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except:
        print("converting kaggle data didnt work")
    
    #clean ratings data
    pd.to_datetime(ratings['timestamp'], unit='s')
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    
    #merge dataframes
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    #clean up duplicated columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    movies_df['runtime'] = movies_df.apply(lambda row: row['running_time'] if row['runtime'] == 0 else row['runtime'], axis=1)
    movies_df['Budget'] = movies_df.apply(lambda row: row['budget'] if row['Budget'] == 0 else row['Budget'], axis=1)   
    movies_df['revenue'] = movies_df.apply(lambda row: row['Box_office'] if row['revenue'] == 0 else row['revenue'], axis=1)
    
    movies_df.drop(columns='running_time', inplace=True)
    movies_df.drop(columns='budget', inplace=True)
    movies_df.drop(columns='Box_office', inplace=True)
    
    #re-order columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','Budget','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    
    #rename columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
    #clean and join rating information
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    #connect to sqldb

    #ASSUMPTION: catching an error that could happenn when opening a sql connection

    try:
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
    except:
        print('Could not connect to database')
        
    
    #delete data from table and then import movie data

    #ASSUMPTION catching an error that could happen when inserting and deleting data from sql
    try:
        with engine.connect() as con:
            con.execute(text('DELETE FROM movies'))
            
        movies_df.to_sql(name='movies', con=engine, if_exists='append')
    except:
        print('Could not delete and import movie data')
        
        
    #ASSUMPTION catching an error that could happen when inserting and deleting data from sql
    try:
        with engine.connect() as con:
            con.execute(text('DELETE FROM ratings'))
            
        rows_imported = 0

        for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000): 
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
    except:
        print('Could not delete and import ratings data')
    
#make the call    
get_the_data_to_sql(wiki_movies_raw,kaggle_metadata,ratings)    