# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 14:47:01 2020

@author: Leonid
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import datetime 

try:
    conn = psycopg2.connect(dbname='cdto_platform', user='extuser', 
                    password='Lfgg&344h$@&*', host='10.8.1.4', port="5432")

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
except:
    print('Ошибка подключения к базе данных')
    

try:
    df_rez3= pd.read_sql_query('''
    SELECT  tab3."id" AS "student_id",
            tab1."caption" AS "syllabus",
            tab2."caption" AS "study_group",
            tab2."syllabus_id",
            tab4."topic_poll_id",
            tab5."caption" AS "topic_poll_title",
            tab5."is_required" AS "topic_poll_is_required",
            tab4."was_skipped" AS "topic_poll_was_skipped",
            tab4."created_at",
            tab4."finished_at",
            tab7."id" AS question_id,
            tab7."question",
            tab7."type" AS "question_type",
            tab7."answers",
            tab6."answers" AS "student_answers"
            
                
        FROM syllabuses AS tab1
            
            INNER JOIN public.study_groups AS tab2 
            ON tab1."id"=tab2."syllabus_id"
            
            INNER JOIN public.students AS tab3 
            ON tab2."id"=tab3."study_group_id"
            
            INNER JOIN public.student_topic_poll_contexts AS tab4
            ON tab3."id"=tab4."student_id"
            
            INNER JOIN public.topic_polls AS tab5
            ON tab5."id"=tab4."topic_poll_id"
            
            INNER JOIN public.student_topic_poll_question_contexts AS tab6
            ON (tab6."student_id"=tab3."id") AND (tab6."topic_poll_context_id"=tab4."id") 
            
            INNER JOIN public.topic_poll_questions AS tab7
            ON tab7."id"=tab6."topic_poll_question_id" AND tab5."id"=tab7."topic_poll_id"
        
        
        WHERE (tab7."type" = 'free') AND (tab4."created_at" BETWEEN '{0}' AND '{1}')
        --LIMIT 10000
           '''.format('2021-03-01', '2021-08-01'),
        conn)
except:
    print('Ошибка чтения из базы')

conn.close()


# for name in list(rez3.columns):
#     print(name, type(rez3[name].iloc[0]))