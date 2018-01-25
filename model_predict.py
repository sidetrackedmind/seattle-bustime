import numpy as np
import pickle
import boto3
import os
from sklearn.ensemble import GradientBoostingRegressor


def model_predict(X_array, pickle_path):
    '''
    INPUT
    -------
    X_array = user_input reformatted for the model
    pickle_path on s3 bucket
    Output:
    -------
    prediction
    '''
    fit_model = get_pickle_model(pickle_path)

    prediction = fit_model.predict(X_array)

    return prediction[0]



def get_pickle_model(filename):
        '''
        INPUT
        -------
        filename = pickle_path on s3 bucket
        Output:
        -------
        fit_model
        '''
        bucket_name = os.environ["BUS_BUCKET_NAME"]
        s3 = boto3.client('s3')

        pickled_model = s3.get_object(Bucket=bucket_name,
                                        Key=filename)

        pickle_string = pickled_model['Body'].read()
        fit_model = pickle.loads(pickle_string)

        return fit_model
