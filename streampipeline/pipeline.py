

class preprocess(beam.Ppreprocess)
def remove_novariance(gs_data, project_id, bucket_name, threshold = 0):
    df = convert_df(gs_data)
    X = df.loc[:, df.columns != 'RUL']

    # Fit the feature selection method
    variance_selector = feature_selection.VarianceThreshold(threshold= threshold)
    variance_selector.fit(X)

    # Save the selector in bucket
    save_model(variance_selector, project_id, bucket_name, 'variance_selector.joblib')

    # Apply selector on training data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns = X.columns.values[columns_variance])

    df = pd.concat([X, df['RUL']], axis =1).to_csv()
    yield df #convert.to_pcollection(df)