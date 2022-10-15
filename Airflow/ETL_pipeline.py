# coding=utf-8

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pandahouse as ph

config = load(open('config.yaml'), Loader=FullLoader)

connection = {
    'host': config['config']['host'],
    'database': 'simulator_20220820',
    'user': 'student',
    'password': config['config']['password']
}

connection_2 = {
    'host': config['config']['host'],
    'database': 'test',
    'user': 'student-rw',
    'password': config['config']['password_2']
}


def select(query):
    return ph.read_clickhouse(query, connection=connection)


default_args = {
    'owner': 'r-janbuhtin-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1)
}

schedule_interval = '0 23 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_yanb_less():

    @task()
    def extract_feed():
        query = """
                SELECT user_id,
                       gender,
                       age,
                       os,
                       toDate(time) AS event_date,
                       countIf(action = 'view') AS views,
                       countIf(action = 'like') AS likes
                FROM {db}.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, gender, age, os, toDate(time)
                """
        df_feed = select(query)
        return df_feed

    @task()
    def extract_messages():
        query = """
                WITH messages_and_users_received AS (
                        SELECT DISTINCT reciever_id,
                               toDate(time) AS event_date,
                               COUNT(user_id) AS messages_received,
                               COUNT(DISTINCT user_id) AS users_received
                        FROM {db}.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY reciever_id, toDate(time)),
        
                messages_and_users_sent AS (
                        SELECT DISTINCT user_id,
                               toDate(time) AS event_date,
                               COUNT(reciever_id) AS messages_sent,
                               COUNT(DISTINCT reciever_id) AS users_sent
                        FROM {db}.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY user_id, toDate(time))
        
                SELECT user_id,
                       messages_received,
                       messages_sent,
                       users_received,
                       users_sent
                FROM messages_and_users_sent s
                LEFT JOIN messages_and_users_received r ON s.user_id = r.reciever_id
                """
        df_messages = select(query)
        return df_messages

    @task
    def merge_df(df_feed, df_messages):
        df = df_feed.merge(df_messages, how='outer', on='user_id')
        return df

    @task
    def gender_metrics(df):
        gender_df = (
            df
            .drop(['user_id', 'age'], axis=1)
            .groupby(['event_date', 'gender'])
            .sum()
            .reset_index()
            .rename({'gender': 'dimension_value'}, axis=1)
        )
        gender_df['dimension'] = 'gender'
        return gender_df

    @task
    def age_metrics(df):
        age_df = (
            df
            .drop(['user_id', 'gender'], axis=1)
            .groupby(['event_date', 'age'])
            .sum()
            .reset_index()
            .rename({'age': 'dimension_value'}, axis=1)
        )
        age_df['dimension'] = 'age'
        return age_df

    @task
    def os_metrics(df):
        os_df = (
            df
            .drop(['user_id', 'gender', 'age'], axis=1)
            .groupby(['event_date', 'os'])
            .sum()
            .reset_index()
            .rename({'os': 'dimension_value'}, axis=1)
        )
        os_df['dimension'] = 'os'
        return os_df

    @task
    def concat_df_metrics(gender_df, age_df, os_df):
        df_concat = pd.concat([gender_df, age_df, os_df],
                              axis=0, ignore_index=True)
        for i in list(df_concat.select_dtypes(['float']).columns):
            df_concat[i] = df_concat[i].astype(int)
        return df_concat

    @task
    def upload_data(df_concat):
        query = """
                CREATE TABLE IF NOT EXISTS test.rus_yanb
                (
                    event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt64,
                    likes UInt64,
                    messages_received UInt64,
                    messages_sent UInt64,
                    users_received UInt64,
                    users_sent UInt64
                ) ENGINE=Log()
                """
        ph.execute(query=query, connection=connection_2)
        ph.to_clickhouse(df=df_concat, table='rus_yanb',
                         index=False, connection=connection_2)

    df_feed = extract_feed()
    df_messages = extract_messages()
    df = merge_df(df_feed, df_messages)
    gender_df = gender_metrics(df)
    age_df = age_metrics(df)
    os_df = os_metrics(df)
    df_concat = concat_df_metrics(gender_df, age_df, os_df)
    upload_data(df_concat)


dag_yanb_less = dag_yanb_less()
