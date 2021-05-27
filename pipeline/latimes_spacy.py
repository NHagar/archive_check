import sqlite3

import pandas as pd
import spacy

from tqdm import tqdm

def process_document(doc):
    pos = set([tok.tag_ for tok in doc])
    
    return pos


if __name__ == "__main__":
    nlp = spacy.load("en_core_web_lg")
    # Load in dataframes
    con = sqlite3.connect("./data/lat.db")
    df = pd.read_sql_query("SELECT * FROM parsed_articles", con)
    services = pd.read_csv("./data/lat_all_urls.csv")
    novdf = services[(services['url'].str.contains("/story/")) &
                (services['url'].str.contains("2020-11"))]
    # Extract parts of speech
    docs = []
    for i in tqdm(nlp.pipe(df['hed'].tolist(), n_process=-1)):
        pos = process_document(i)
        docs.append(pos)

    df.loc[:, "pos"] = docs
    pdf = df[['url', 'pos']]
    pdf = pdf.explode('pos')
    # Save table
    pdf.to_sql("partsofspeech", con, if_exists="append")