#!/bin/bash

# Add AWS keys to environment
export AWS_CREDENTIALS_FILE=$HOME/.aws/credentials.csv
export AWS_ACCESS_KEY_ID=$(tail -n 1 $AWS_CREDENTIALS_FILE | cut -f 2 -d ,)
export AWS_SECRET_ACCESS_KEY=$(tail -n 1 $AWS_CREDENTIALS_FILE | cut -f 3 -d ,)
