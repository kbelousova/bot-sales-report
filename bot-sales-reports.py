#!/usr/bin/env python
# coding: utf-8

# Вы работаете аналитиком в стартапе в области обучения взрослых английскому языку. Компания растет быстро, целый штат маркетологов и продактов придумывает новые механики привлечения пользователей, улучшая коммерческие продукты.
# 
# Ещё есть отдел продаж. В нём сотрудники звонят потенциальным клиентам и продают пакеты уроков. Только вот маркетинг не дружит с продажами и красивой сквозной аналитики у Руководителя отдела продаж нет. Нужно ему помочь.
# 
# Никакого централизованного DWH или сложного BI - у компании нет, но бизнесу точно нужны основные метрики, причем завтра. Данные об основных событиях CJM пользователя записываются в Postgres.
# 
# Руководитель отдела продаж просит вас каждый день присылать в его telegram метрики или графики. Они должны помочь ему понимать как идут дела в отделе, так сказать держать руку на пульсе.
# 
# ### Данные
# 
# Все таблицы с данными в PostgresSQL.
# 
# ## Задачи
# 
# 1. Выберите 3 метрики, которые помогут руководителю отдела продаж контролировать ситуацию ежедневно — всё ли идёт нормально. Объясните свой выбор.
# 2. Напишите Телеграм Бота (скрипт), который будет отправлять ежедневный отчёт по этим метрикам в Телеграм руководителю.
# Чтобы показать, как работает бот вставьте его код в файл с ответом и прикрепите скриншот отправленного им сообщения, чтобы было видно от кого это. Под названием отправителя должно быть написано Бот, как на скриншоте ниже.

# In[ ]:


import pandas as pd 
import psycopg2
import numpy as np
import telebot 
import matplotlib.pyplot as plt
import seaborn as sns
import schedule
import time
import threading
import sqlalchemy as db
from sqlalchemy import text

chat_ids=[]

revenue_last_month_query=text('''
    SELECT sum(value) revenue 
    FROM public.transaction 
    WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction)
    ''')
aov_query=text("""
    SELECT AVG(value) 
    FROM public.transaction 
    WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction)
    """)

arppu_query=text("""SELECT SUM(value)/COUNT(DISTINCT user_id) 
FROM public.transaction 
WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction) 
AND value!=0""")

new_users_query=text('''
        SELECT COUNT(DISTINCT user_id) 
        FROM public.transaction 
        WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction)
        AND user_id not in (SELECT user_id FROM public.transaction WHERE created_at < (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction))
        ''')
       
revenue_lastmonth_query=text("""SELECT SUM(value) revenue, 
DATE_TRUNC('day', created_at) date 
FROM public.transaction 
WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction) 
GROUP BY 2 
ORDER BY 2""")
        
users_lastmonth_query=text("""SELECT COUNT(DISTINCT user_id) users,
DATE_TRUNC('day', created_at) date 
FROM public.transaction 
WHERE DATE_TRUNC('month', created_at) in (SELECT DATE_TRUNC('month', MAX(created_at)) FROM public.transaction) 
GROUP BY 2 
ORDER BY 2""")
 
    
bot=telebot.TeleBot()

def query(text):
    engine = db.create_engine(
        "postgresql+psycopg2://student:qweasd963@95.163.241.236:5432/simulative") 
    with engine.connect() as connection:
        result=connection.execute(text)

        answer = pd.DataFrame(result.fetchall(), columns=result.keys())
    return answer

def preparing_data():
    revenue_last_month=query(revenue_last_month_query).values[0][0]
    aov=round(query(aov_query).values[0][0],2)
    arppu=query(arppu_query).values[0][0]
    new_users=query(new_users_query).values[0][0]
    revenue_lastmonth=query(revenue_lastmonth_query)
    users_lastmonth=query(users_lastmonth_query)
 

    sns.set(rc={'figure.figsize':(15.7, 5.7)})
    plot=sns.barplot(y=revenue_lastmonth.revenue, x=revenue_lastmonth.date.dt.day)
    plt.title('Выручка по дням последнего месяца', fontsize=17)
    plt.xlabel('День')
    plt.ylabel('Выручка')
    plot=plot.get_figure().savefig('Revenue by days.png')
    plt.clf()
        
    
    sns.set(rc={'figure.figsize':(15.7, 5.7)})
    plot=sns.barplot(y=users_lastmonth.users, x=users_lastmonth.date.dt.day)
    plt.title('Уникальные пользователи по дням последнего месяца', fontsize=17)
    plt.xlabel('День')
    plt.ylabel('Количество пользователей')
    plot=plot.get_figure().savefig('Users by days.png')
    return revenue_last_month, aov, arppu, new_users
        
    

@bot.message_handler(commands=['start'])
def hello_send(message):
    chat_id=message.chat.id
    if chat_id not in chat_ids:
        chat_ids.append(chat_id)
    bot.send_message(chat_id, 'Теперь вы будете еженедельно получать отчет по обновляющимся данным о транзакциях за последний месяц')
        
        
def report():
    for chat_id in chat_ids: #надо написать функцию, которая будет возвращать данные из файла + создать функцию, которая их будет собирать
        revenue_last_month, aov, arppu, new_users=preparing_data()
        report_saving = (
                f"Отчет за последний месяц:\n"
                f"Выручка: {revenue_last_month}\n"
                f"Средний чек: {aov}\n"
                f"ARPPU: {arppu}\n"
                f"Новые пользователи: {new_users}")
        bot.send_message(chat_id, report_saving)
        bot.send_photo(chat_id, open('Users by days.png', 'rb'))
        bot.send_photo(chat_id, open('Revenue by days.png', 'rb'))
schedule.every(4).weeks.do(report)

    
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)


scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.start()
bot.polling(none_stop=True, interval=0)


# In[ ]:




