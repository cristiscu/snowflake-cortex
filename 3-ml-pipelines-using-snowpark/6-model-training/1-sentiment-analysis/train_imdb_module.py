from snowflake.snowpark.session import Session

def train_imdb(session: Session, train_dataset_name: str, tmp_folder: str):
    
    from snowflake.snowpark import functions as fn
    import sklearn.feature_extraction.text as txt
    from sklearn import svm
    from joblib import dump
    
    df = session.table(train_dataset_name)
    df_flag = df.withColumn("SENTIMENT_FLAG",
        fn.when(df.SENTIMENT == "positive", 1).otherwise(2))
    train_x = df_flag.toPandas().REVIEW.values
    train_y = df_flag.toPandas().SENTIMENT_FLAG.values
    df_flag.show()

    filename = f'{tmp_folder}vect_review1.joblib'
    print(f'Building Sparse Matrix into {filename}...')
    vector = txt.CountVectorizer(
        token_pattern="[\\w']+\\w\\b", ngram_range=(1, 2), analyzer='word', 
        max_df=0.02, min_df=1 * 1./len(train_x), vocabulary=None, binary=True)
    bow = vector.fit_transform(train_x)
    dump(vector, filename, compress=True)
    session.file.put(filename, "@models", auto_compress=True, overwrite=True)

    filename = f'{tmp_folder}model_review1.joblib'
    print(f'Fitting model into {filename}...')
    model = svm.LinearSVC(C=1.8, max_iter=100)
    model.fit(bow, train_y)
    dump(model, filename, compress=True)
    session.file.put(filename, "@models", auto_compress=True, overwrite=True)

    return { "STATUS": "SUCCESS", "R2 Score Train": str(model.score(bow, train_y)) }