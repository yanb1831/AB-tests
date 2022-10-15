# coding=utf-8

# импорты
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

config = load(open('config.yaml'), Loader=FullLoader)

connection = {
    'host': config['config']['host'],
    'database': 'simulator_20220820',
    'user': 'student',
    'password': config['config']['password']
}

default_args = {
    'owner': 'r-janbuhtin-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 1)
}

schedule_interval = '0 11 * * *'

# функции


def select(query):
    return ph.read_clickhouse(query, connection=connection)


def d_type(data):
    for _ in list(data.select_dtypes(['uint64']).columns):
        data[_] = data[_].astype(int)
    return data


def up_down(value):
    if value > 0:
        a = ''' ⬆️'''
    elif value < 0:
        a = '''⬇️'''
    else:
        a = '''0️⃣'''
    return a


def v(a, b):
    c = (b-a)/a
    return c


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_yanb_7les():

    chat_id = config['config']['chat_id']
    token = config['config']['token']
    bot = telegram.Bot(token)

    @task
    def data_extract_feed():
        query = '''
        SELECT toDate(time) AS date,
               COUNT(DISTINCT post_id) AS posts,
               countIf(action = 'view') AS views,
               countIf(action = 'like') AS likes,
               views + likes AS reactions,
               COUNT(DISTINCT user_id) AS DAU,
               100 * likes / views AS CTR,
               likes / DAU AS likes_user
        FROM {db}.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 and yesterday()
        GROUP BY date
        '''
        data = d_type(select(query))
        return data

    @task
    def data_extract_messages():
        query = '''
        SELECT DISTINCT toDate(time) AS date,
               COUNT(DISTINCT user_id) AS DAU_messages,
               COUNT(user_id) AS messages,
               COUNT(DISTINCT reciever_id) AS recieved_messages,
               messages / DAU_messages AS messages_per_user,
               recieved_messages / DAU_messages AS recieved_per_user
        FROM {db}.message_actions
        WHERE toDate(time) BETWEEN today() - 7 and yesterday()
        GROUP BY date
        '''
        data_2 = d_type(select(query))
        return data_2

    @task
    def data_extract_1():
        query = '''
        WITH t as(
        SELECT DISTINCT toDate(time) AS date,
               user_id,
               os,
               source,
               gender
        FROM {db}.feed_actions
        WHERE toDate(time) BETWEEN today() - 7 and yesterday()
        UNION ALL
        SELECT DISTINCT toDate(time) AS date,
               user_id,
               os,
               source,
               gender
        FROM {db}.message_actions
        WHERE toDate(time) BETWEEN today() - 7 and yesterday())
    
        SELECT date, 
               uniqExact(user_id) AS users,
               uniqExactIf(user_id, os='iOS') AS iOS,
               uniqExactIf(user_id, os='Android') AS Android,
               uniqExactIf(user_id, source='ads') AS ads,
               uniqExactIf(user_id, source='organic') AS organic,
               uniqExactIf(user_id, gender='0') AS g_0,
               uniqExactIf(user_id, gender='1') AS g_1
        FROM t
        GROUP BY date
        '''
        data_3 = d_type(select(query))
        return data_3

    @task
    def data_extract_2():
        query = '''
        WITH t AS (
        SELECT user_id,
               MIN(toDate(time)) AS min_date,
               source
        FROM {db}.feed_actions
        WHERE toDate(time) BETWEEN today() - 120 and yesterday()
        GROUP BY user_id,source
        UNION ALL
        SELECT user_id,
               MIN(toDate(time)) AS min_date,
               source
        FROM {db}.message_actions
        WHERE toDate(time) BETWEEN today() - 120 and yesterday()
        GROUP BY user_id,source),
        t_2 AS (
        SELECT user_id,
               min(min_date) AS date,
               source
        FROM t
        GROUP BY user_id,source)
        SELECT date,
               uniqExact(user_id) AS new_users,
               uniqExactIf(user_id,source='ads') AS new_users_ads,
               uniqExactIf(user_id,source='organic') AS new_users_organic
        FROM t_2
        WHERE date BETWEEN today() - 7 and yesterday()
        GROUP BY date
        '''
        data_4 = d_type(select(query))
        return data_4

    @task
    def data_merge(data, data_2, data_3, data_4):
        df_1 = (pd
                .merge(data_3, data_4, on='date')
                .rename({'iOS': 'users_iOS',
                         'Android': 'users_Android',
                         'g_0': 'gender_0',
                         'g_1': 'gender_1',
                         }, axis=1))
        df_2 = pd.merge(data, data_2, on='date')
        df = pd.merge(df_1, df_2, on='date')
        return df

    @task
    def telegram_bot_message(df):
        yesterday = (pd.to_datetime('today')-timedelta(days=1)
                     ).normalize().strftime("%Y-%m-%d")
        day_before_yesterday = (pd.to_datetime(
            'today')-timedelta(days=2)).normalize().strftime("%Y-%m-%d")

        df_yesterday = df.query('date == @yesterday')
        df_before_yesterday = df.query('date == @day_before_yesterday')

        message = '''
        Отчет за <b>{date}</b>

        ➖➖➖ ЛЕНТА НОВОСТЕЙ ➖➖➖
        <b>DAU</b>: {DAU} ({dau_v:+.2%} {dau_s})
        <b>просмотры</b>: {views} ({views_v:+.2%} {views_s})
        <b>лайки</b>: {likes} ({likes_v:+.2%} {likes_s})
        <b>лайки на пользователя</b>: {likes_per_user:.1f} ({likes_per_user_v:+.2%} {likes_per_user_s})
        <b>реакции</b>: {reactions} ({reactions_v:+.2%} {reactions_s})
        <b>посты</b>: {posts} ({posts_v:+.2%} {posts_s})
        <b>CTR</b>: {CTR:.1f} ({CTR_v:+.2%} {CTR_s})

        ➖➖➖ СЕРВИС СООБЩЕНИЙ ➖➖➖
        <b>DAU</b>: {users_m} ({users_m_v:+.2%} {users_m_s})
        <b>сообщения</b>: {msg} ({msg_v:+.2%} {msg_s})
        <b>сообщения на пользователя</b>: {msg_per_user:.1f} ({msg_per_user_v:+.2%} {msg_per_user_s})

        ➖➖➖ ОБЩАЯ ИНФОРМАЦИЯ ➖➖➖
        <b>Всего событий</b>: {events}
        <b>DAU</b>: {DAU_ALL} ({DAU_ALL_v:+.2%} {DAU_ALL_s})
        <b>DAU</b> по платформам:
            - <b>iOS</b> {iOS} ({iOS_v:+.2%} {iOS_s})
            - <b>Android</b> {android} ({android_v:+.2%} {android_s})
        <b>новые пользователи</b>: {new_u} ({new_u_v:+.2%} {new_u_s})
        <b>новые пользователи</b> по каналам:
            - <b>ads</b> {new_u_a} ({new_u_a_v:+.2%} {new_u_a_s})
            - <b>organic</b> {new_u_o} ({new_u_o_v:+.2%} {new_u_o_s})
        '''

        report = message.format(
            date=yesterday,

            DAU=df_yesterday['DAU'].iloc[0],
            dau_v=v(df_before_yesterday['DAU'].iloc[0],
                    df_yesterday['DAU'].iloc[0]),
            dau_s=up_down(
                v(df_before_yesterday['DAU'].iloc[0], df_yesterday['DAU'].iloc[0])),

            views=df_yesterday['views'].iloc[0],
            views_v=v(
                df_before_yesterday['views'].iloc[0], df_yesterday['views'].iloc[0]),
            views_s=up_down(
                v(df_before_yesterday['views'].iloc[0], df_yesterday['views'].iloc[0])),

            likes=df_yesterday['likes'].iloc[0],
            likes_v=v(
                df_before_yesterday['likes'].iloc[0], df_yesterday['likes'].iloc[0]),
            likes_s=up_down(
                v(df_before_yesterday['likes'].iloc[0], df_yesterday['likes'].iloc[0])),

            likes_per_user=df_yesterday['likes_user'].iloc[0],
            likes_per_user_v=v(
                df_before_yesterday['likes_user'].iloc[0], df_yesterday['likes_user'].iloc[0]),
            likes_per_user_s=up_down(
                v(df_before_yesterday['likes_user'].iloc[0], df_yesterday['likes_user'].iloc[0])),

            reactions=df_yesterday['reactions'].iloc[0],
            reactions_v=v(
                df_before_yesterday['reactions'].iloc[0], df_yesterday['reactions'].iloc[0]),
            reactions_s=up_down(
                v(df_before_yesterday['reactions'].iloc[0], df_yesterday['reactions'].iloc[0])),

            posts=df_yesterday['posts'].iloc[0],
            posts_v=v(
                df_before_yesterday['posts'].iloc[0], df_yesterday['posts'].iloc[0]),
            posts_s=up_down(
                v(df_before_yesterday['posts'].iloc[0], df_yesterday['posts'].iloc[0])),

            CTR=df_yesterday['CTR'].iloc[0],
            CTR_v=v(df_before_yesterday['CTR'].iloc[0],
                    df_yesterday['CTR'].iloc[0]),
            CTR_s=up_down(
                v(df_before_yesterday['CTR'].iloc[0], df_yesterday['CTR'].iloc[0])),

            users_m=df_yesterday['DAU_messages'].iloc[0],
            users_m_v=v(
                df_before_yesterday['DAU_messages'].iloc[0], df_yesterday['DAU_messages'].iloc[0]),
            users_m_s=up_down(
                v(df_before_yesterday['DAU_messages'].iloc[0], df_yesterday['DAU_messages'].iloc[0])),

            msg=df_yesterday['messages'].iloc[0],
            msg_v=v(df_before_yesterday['messages'].iloc[0],
                    df_yesterday['messages'].iloc[0]),
            msg_s=up_down(
                v(df_before_yesterday['messages'].iloc[0], df_yesterday['messages'].iloc[0])),

            msg_per_user=df_yesterday['messages_per_user'].iloc[0],
            msg_per_user_v=v(
                df_before_yesterday['messages_per_user'].iloc[0], df_yesterday['messages_per_user'].iloc[0]),
            msg_per_user_s=up_down(
                v(df_before_yesterday['messages_per_user'].iloc[0], df_yesterday['messages_per_user'].iloc[0])),

            events=df_yesterday['reactions'].iloc[0] +
            df_yesterday['messages'].iloc[0],

            DAU_ALL=df_yesterday['users'].iloc[0],
            DAU_ALL_v=v(
                df_before_yesterday['users'].iloc[0], df_yesterday['users'].iloc[0]),
            DAU_ALL_s=up_down(
                v(df_before_yesterday['users'].iloc[0], df_yesterday['users'].iloc[0])),

            iOS=df_yesterday['users_iOS'].iloc[0],
            iOS_v=v(df_before_yesterday['users_iOS'].iloc[0],
                    df_yesterday['users_iOS'].iloc[0]),
            iOS_s=up_down(
                v(df_before_yesterday['users_iOS'].iloc[0], df_yesterday['users_iOS'].iloc[0])),

            android=df_yesterday['users_Android'].iloc[0],
            android_v=v(
                df_before_yesterday['users_Android'].iloc[0], df_yesterday['users_Android'].iloc[0]),
            android_s=up_down(
                v(df_before_yesterday['users_Android'].iloc[0], df_yesterday['users_Android'].iloc[0])),

            new_u=df_yesterday['new_users'].iloc[0],
            new_u_v=v(
                df_before_yesterday['new_users'].iloc[0], df_yesterday['new_users'].iloc[0]),
            new_u_s=up_down(
                v(df_before_yesterday['new_users'].iloc[0], df_yesterday['new_users'].iloc[0])),

            new_u_a=df_yesterday['new_users_ads'].iloc[0],
            new_u_a_v=v(
                df_before_yesterday['new_users_ads'].iloc[0], df_yesterday['new_users_ads'].iloc[0]),
            new_u_a_s=up_down(
                v(df_before_yesterday['new_users_ads'].iloc[0], df_yesterday['new_users_ads'].iloc[0])),

            new_u_o=df_yesterday['new_users_organic'].iloc[0],
            new_u_o_v=v(df_before_yesterday['new_users_organic'].iloc[0],
                        df_yesterday['new_users_organic'].iloc[0]),
            new_u_o_s=up_down(
                v(df_before_yesterday['new_users_organic'].iloc[0], df_yesterday['new_users_organic'].iloc[0])),
        )
        bot.sendMessage(chat_id=chat_id, text=report, parse_mode="html")

    @task
    def telegram_bot_plot(df):
        fig, axes = plt.subplots(2, 2, figsize=(16, 14))
        fig.suptitle('Метрики ленты новостей за прошлые 7 дней',
                     fontweight='bold', fontsize=16)

        list_column = ['DAU', 'views', 'likes', 'CTR']

        for ax, column in zip(axes.flatten(), list_column):
            sns.lineplot(data=df, x='date', y=column, ax=ax,
                         color='white', linewidth=2.5)
            ax.set_title('{}'.format(column), fontweight='bold', fontsize=12)
            if column == 'DAU':
                ax.yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.1f}k'.format(y/1000)))
            elif column == 'likes':
                ax.yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.0f}k'.format(y/1000)))
            elif column == 'views':
                ax.yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.0f}k'.format(y/1000)))
            else:
                pass
            ax.set(xlabel=None, ylabel=None)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'feed_metrics.png'
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        fig, axes = plt.subplots(3, figsize=(16, 14))
        fig.suptitle('Метрики сервиса сообщений за прошлые 7 дней',
                     fontweight='bold', fontsize=16)

        list_column = ['DAU_messages', 'messages', 'messages_per_user']

        for ax, column in zip(axes.flatten(), list_column):
            sns.lineplot(data=df, x='date', y=column, ax=ax,
                         color='white', linewidth=2.5)
            ax.set_title('{}'.format(column), fontweight='bold', fontsize=12)
            ax.set(xlabel=None, ylabel=None)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            if column == 'DAU':
                ax.yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.1f}k'.format(y/1000)))
            elif column == 'messages':
                ax.yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.1f}k'.format(y/1000)))
            else:
                pass

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'messages_metrics.png'
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        fig, axes = plt.subplots(4, figsize=(
            16, 18), gridspec_kw={'hspace': 0.3})
        fig.suptitle('Общие метрики за прошлые 7 дней',
                     fontweight='bold', fontsize=16)

        plot_dict = {
            0: {'y': ['users', 'users_iOS', 'users_Android'], 'title': 'DAU os'},
            1: {'y': ['users', 'gender_0', 'gender_1'], 'title': 'DAU gender'},
            2: {'y': ['new_users', 'new_users_ads', 'new_users_organic'], 'title': 'new users'},
            3: {'y': ['users', 'ads', 'organic'], 'title': 'users channel'},
        }

        for i in range(4):
            for y in plot_dict[i]['y']:
                sns.lineplot(data=df, x='date', y=y, ax=axes[i], label=y)
                axes[i].set_title(plot_dict[i]['title'])
                axes[i].yaxis.set_major_formatter(FuncFormatter(
                    lambda y, pos: '{:.1f}k'.format(y/1000)))
                axes[i].xaxis.set_major_formatter(
                    mdates.DateFormatter('%m-%d'))
                axes[i].set(xlabel=None, ylabel=None)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'all_metrics.png'
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    data = data_extract_feed()
    data_2 = data_extract_messages()
    data_3 = data_extract_1()
    data_4 = data_extract_2()
    df = data_merge(data, data_2, data_3, data_4)
    telegram_bot_message(df)
    telegram_bot_plot(df)


dag_report_yanb_7les = dag_report_yanb_7les()
