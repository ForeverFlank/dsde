import os
import pandas as pd
import json

is_student = "STUDENT" in os.environ

videos_csv_path       = "./videos.csv"       if is_student else "/data/videos.csv"
category_id_json_path = "./category_id.json" if is_student else "/data/category_id.json"

"""
    ASSIGNMENT 1 (STUDENT VERSION):
    Using pandas to explore youtube trending data from (videos.csv and category_id.json) and answer the questions.
"""

def Q1():
    """
        1. How many rows are there in the videos.csv after removing duplications?
        - To access 'videos.csv', use the path '/data/videos.csv'.
    """
    vdo_df = pd.read_csv(videos_csv_path)
    deduped = vdo_df.drop_duplicates()
    return deduped.shape[0]

def Q2(vdo_df):
    '''
        2. How many VDO that have "dislikes" more than "likes"? Make sure that you count only unique title!
            - videos.csv has been loaded into memory and is ready to be utilized as vdo_df
            - The duplicate rows of vdo_df have been removed.
    '''
    return len(vdo_df[vdo_df["dislikes"] > vdo_df["likes"]].value_counts("title"))

def Q3(vdo_df):
    '''
        3. How many VDO that are trending on 22 Jan 2018 with comments more than 10,000 comments?
            - videos.csv has been loaded into memory and is ready to be utilized as vdo_df
            - The duplicate rows of vdo_df have been removed.
            - The trending date of vdo_df is represented as 'YY.DD.MM'. For example, January 22, 2018, is represented as '18.22.01'.
    '''
    return vdo_df[(vdo_df["trending_date"] == "18.22.01") & (vdo_df["comment_count"] > 10_000)].shape[0]

def Q4(vdo_df):
    '''
        4. Which trending date that has the minimum average number of comments per VDO?
            - videos.csv has been loaded into memory and is ready to be utilized as vdo_df
            - The duplicate rows of vdo_df have been removed.
    '''
    by_trending_dates = vdo_df.groupby(["trending_date"])
    max_idx = by_trending_dates["comment_count"].mean().idxmin()
    return max_idx

def Q5(vdo_df):
    '''
        5. Compare "Sports" and "Comedy", how many days that there are more total daily views of VDO in "Sports" category than in "Comedy" category?
            - videos.csv has been loaded into memory and is ready to be utilized as vdo_df
            - The duplicate rows of vdo_df have been removed.
            - You must load the additional data from 'category_id.json' into memory before executing any operations.
            - To access 'category_id.json', use the path '/data/category_id.json'.
    '''
    vdo_df["publish_time"] = pd.to_datetime(vdo_df["publish_time"]).dt.date

    cat_df = pd.read_json(category_id_json_path)
    cat_df = pd.json_normalize(cat_df["items"])
    cat_df = cat_df.astype({ "id": int })
    
    joined = pd.merge(vdo_df, cat_df, left_on="category_id", right_on="id")

    views = (
        joined.groupby(["trending_date", "snippet.title"])["views"]
        .sum()
        .unstack(fill_value=0)
    )

    return (views["Sports"] > views["Comedy"]).sum()


