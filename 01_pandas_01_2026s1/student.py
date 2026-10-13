import pandas as pd
import json

def Q1(df):
    return df.shape

def Q2(df):
    return df["score"].max()
    
def Q3(df):
    return df[df["score"] >= 80].shape[0]

def Q4(df):
    return "No Output"

