# -*- coding: utf-8 -*-
"""
Created on Tue Jul 13 14:12:38 2021
Файл содержит описание кластера извлекающего выборку ответов слушателей на открытые вопросы 
данные выгружаются из платформы и грузятся в таблицу 'quiz.poll_result_analysis'

Схема работы:
        etl_qwe=load_transform_free_qwe() - создаем объект класса
        etl_qwe.load_data(date=['2021-03-01', '2021-04-01']) - загружаем в указанные данные все вопросы и иответы
        etl_qwe.transform() - преобразуем данные. извлекаем ответы
        etl_qwe.save_data(add=False) - сохраняем данные в DWH
  

@author: Leonid
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
 


def unpacking_student_answers(ans):
    
    if ans==None:
        return None
    return ans[0]['text']

class load_transform_free_qwe:
    def __init__(self):
        self.Name_table='quiz.poll_result_analysis'
        return
    
    def load_data(self, date):
        try:
            conn = psycopg2.connect(dbname='cdto_platform', user='extuser', 
                            password='Lfgg&344h$@&*', host='10.8.1.4', port="5432")
    
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print('Ошибка подключения к базе данных')
            return False
        
        try:
            self.df= pd.read_sql_query('''
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
                   '''.format(date[0], date[1]),
                conn)
        except:
            print('Ошибка чтения из базы')
            return False
        
        conn.close()

        return True
    
    def transform(self):
        try:
            self.df['student_answers']=self.df['student_answers'].apply(unpacking_student_answers)
            self.df['answers']=self.df['answers'].apply(str)
            
        except:
            print('Ошибка извлечения ответа')
            return False
        
        return True
    
    def execute_values(self, conn, df, table):
        """
        Using psycopg2.extras.execute_values() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        tuples = [[y if y==y else None for y in list(x)] for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
       
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            psycopg2.extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("execute_values() done")
        cursor.close()
        return
        
    def save_data(self, add=False):
        #Параметр add говорит о том в каком режиме мы сохраняем данные
        #если add=False, то перезаписываем таблицу, иначе дозаписываем
        print('Сохранение данных {0}'.format(str(datetime.now())))
        
        try:
            conn = psycopg2.connect(dbname='cdto_platform', user='extuser', 
                                    password='Lfgg&344h$@&*', host='10.8.1.3', port="5432")
            
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print("Ошибка подключения к базе")
            return
        
        if add==False:
            try:
                rez=cursor.execute("""
                delete from {0};
                """.format(self.Name_table) )
            except:
                print("Ошибка очистки таблицы")
                conn.close()
                return False
            
        try:
            self.execute_values( conn, self.df, self.Name_table)
            conn.commit()        
                    
        except:
            print("Ошибка записи данных")
            conn.close()
            return False
        
        conn.close()
        
        print('Сохранение данных завершено {0}'.format(str(datetime.now())))
        return True
        

if __name__=='__main__':
    etl_qwe=load_transform_free_qwe()
    etl_qwe.load_data(date=['2021-03-01', '2021-08-01'])
    etl_qwe.transform()
    etl_qwe.save_data(add=False)
    
    