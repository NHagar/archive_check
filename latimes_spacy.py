import sqlite3

import pandas as pd
import spacy

from tqdm import tqdm

def process_document(doc):
    entities = [{"text": ent.text, "type": ent.label_} for ent in doc.ents]
    
    return entities


if __name__ == "__main__":
    nlp = spacy.load("en_core_web_lg", disable=["tok2vec", "parser"])
    # Load in dataframes
    con = sqlite3.connect("./data/lat.db")
    df = pd.read_sql_query("SELECT * FROM parsed_articles", con)
    services = pd.read_csv("./data/lat_all_urls.csv")
    novdf = services[(services['url'].str.contains("/story/")) &
                (services['url'].str.contains("2020-11"))]
    # Extract entities
    docs = []
    for i in tqdm(nlp.pipe(df['text'].tolist(), n_process=-1)):
        entities = process_document(i)
        docs.append(entities)
    # Build entities dataframe
    for i in enumerate(docs):
        for j in i[1]:
            j['url'] = df.loc[i[0], "url"]
    docs = [i for l in docs for i in l]
    docs_df = pd.DataFrame(docs)
    # Save entities table
    docs_df.to_sql("entities", con, if_exists="append")