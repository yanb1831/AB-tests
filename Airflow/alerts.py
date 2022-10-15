# coding=utf-8

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import telegram
import io
import pandahouse as ph
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates

# стили
plt.style.use('dark_background')

# настройки
default_args = {
    'owner': 'r-janbuhtin-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1)
}

schedule_interval = '*/15 * * * *'

config = load(open('config.yaml'), Loader=FullLoader)

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20220820',
    'user': 'student',
    'password': config['config']['password']
}


def select(query):
    return ph.read_clickhouse(query, connection=connection)


def d_type(data):
    for _ in list(data.select_dtypes(['uint64']).columns):
        data[_] = data[_].astype(int)
    return data


def check(df, metric, a=4, n=5):
    df['q25'] = df[metric].shift().rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift().rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df['low'].iloc[-1] > df['up'].iloc[-1]:
        alert = 1
    else:
        alert = 0
    return alert, df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_yanb():

    chat_id = -555114317
    token = '5746137496:AAHL76bcZ9px_SbS3SIo_Zp-znTrMK0tDrA'
    bot = telegram.Bot(token)

    @task
    def extract_data():
        query = '''
        WITH feed_table AS (
        SELECT toStartOfFifteenMinutes(time) AS ts,
                toDate(time) as date,
                formatDateTime(ts, '%R') AS hm,
                uniqExact(user_id) AS users_feed,
                countIf(user_id, action='view') AS views,
                countIf(user_id, action='like') AS likes
        FROM {db}.feed_actions
        WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
        GROUP BY ts,date,hm),
        messages_table AS (
        SELECT toStartOfFifteenMinutes(time) AS ts,
               uniqExact(user_id) AS users_messages,
               COUNT(user_id) AS messages,
               uniqExact(reciever_id) AS recieved_messages,
               countIf(user_id, source='organic') AS organic_users_messages,
               countIf(user_id, source='ads') AS ads_users_messages,
               countIf(user_id, os='iOS') AS iOS_users_messages,
               countIf(user_id, os='Android') AS Android_users_messages
        FROM {db}.message_actions
        WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
        GROUP BY ts)
        SELECT *
        FROM feed_table f
        JOIN messages_table m ON f.ts = m.ts
        ORDER BY ts
        '''
        data = d_type(select(query)).drop('m.ts', axis=1)
        return data

    @task
    def alert(data, chat_id, bot, token):
        metrics_list = list(data.columns[3:])
        for metric in metrics_list:
            df = data[['ts', 'date', metric]].copy()
            alert, df = check(df, metric)

            if alert == 1:
                msg = '''❗️Метрика <b>{metric}</b> \n- текущее значение <b>{val:.2f}</b> ({last_val:.2%})'''.format(
                    metric=metric,
                    val=df[metric].iloc[-1],
                    last_val=abs(
                        1 - (df[metric].iloc[-1]) / df[metric].iloc[-2])
                )

                fig, ax = plt.subplots(figsize=(20, 14))
                sns.lineplot(x=df['ts'], y=df[metric],
                             color='yellow', linewidth=2.5, label='metric')
                sns.lineplot(x=df['ts'], y=df['up'],
                             color='white', linewidth=2.5, label='up')
                sns.lineplot(x=df['ts'], y=df['low'], color='white',
                             linewidth=2.5, label='low', alpha=0.7)
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%H'))
                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(f'Метрика: {metric}')
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.name = '{0}.png'.format(metric)
                plot_object.seek(0)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="html")
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    data = extract_data()
    alert = alert(data, chat_id, bot, token)


alerts_yanb = alerts_yanb()
