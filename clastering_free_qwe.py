# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 13:37:29 2021

В данном моделуе реализован функционал кластеризации ответов на открытые вопросы
Общая схема:
    -подключение к таблице quiz.poll_result_analysis
    -извлечение всех ответов на вопрос по id вопроса. (параметр question_id)
    -кластеризуем полученные ответы
    -результат записываем в таблицу ???

@author: Leonid
"""
from sklearn.pipeline import Pipeline
from my_clasterizator2 import KMeansClusters2
from my_vectorizer import vectorizer

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
 
from sklearn.model_selection import GridSearchCV
from my_split import my_cv_spliter
from sklearn.metrics import silhouette_samples, silhouette_score

from morph_anlysis_me import morph_anlysis_me

class clastering_free_qwe:
    def __init__(self):
        self.Name_table_from='quiz.poll_result_analysis'
        self.Name_table_to='quiz.poll_result_cl_free_qwe'
        self.flag_load=False
        self.df=[]
        return
    

    def load_data(self, question_id):
        #затягиваем ответы для вопроса с id =  question_id
        try:
            conn = psycopg2.connect(dbname='cdto_platform', user='extuser', 
                                    password='Lfgg&344h$@&*', host='10.8.1.3', port="5432")
            
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print("Ошибка подключения к базе")
            return False
        
        self.df = pd.read_sql_query(
            '''SELECT 
                    tab1."student_id",
                    tab1."syllabus",
                    tab1."study_group",
                    tab1."topic_poll_title",
                    tab1."question_id",
                    tab1."question",
                    tab1."student_answers"
               FROM {0} AS tab1
               WHERE tab1."question_id"={1}
            '''.format(self.Name_table_from, question_id),conn)
        
        self.question_id=question_id
        self.flag_load=True
        return True
    
    def transform(self):
        #Предполагается, что данные уже затянуты в переменную self.df (self.flag_load==True)
        #там собраны ответы по одному вопросуv
        if self.flag_load==False:
            return False
        try:
            if len(self.df)<10:
                return False
        except:
            return False
        ind=self.df['student_answers'].notnull()
        self.df_cl=self.df[ind].copy()
        Result=list(self.df_cl['student_answers']) 
        Result=morph_anlysis_me( Result)
        pipe = Pipeline([("vect", vectorizer(method='tfidf', min_df=0.03, max_df=0.9, ngram_range=(1,1) )),
                          ("c1", KMeansClusters2(n_clusters=10, f_opt=False, max_iter=300, n_init=100)),
                           
                         ])
        
        param_grid = {'vect__min_df': [0.01,   0.02, 0.03 ],
                      'vect__max_df': [0.85, 0.9, 0.95],
                      'vect__method':['tfidf', 'bw'],
                       
                      'c1__n_clusters': [ 5, 6, 8,  10, 11, 13,15]
                       }
        grid = GridSearchCV(pipe, param_grid, return_train_score=True, cv=my_cv_spliter )
        
        grid=grid.fit(Result)
        print(grid.best_params_)
        self.best_params_= grid.best_params_
        
        pipe=grid.best_estimator_
        
        X11=pipe.predict(Result)
        self.score=pipe.score(Result)
        cols_in=pipe[-1].get_inputs(X11)
        
        key_words=[]
        X12=X11[cols_in].copy()
        for i in range(len(X12)):
            xx=X12.iloc[i, :].sort_values(ascending=False)
            xx=xx.sort_values(ascending=False)
            xx=xx[xx>0]
            xx1=list(xx.index)
            xx2=str(xx1)
            xx2=xx2.replace('[', '')
            xx2=xx2.replace(']', '')
            xx2=xx2.replace("'", '')
            key_words.append(xx2)
        
        
        self.df_cl['key_words']=key_words
        #прикрепляем метки кластеров
        self.df_cl['cl']=X11['level_0'].values
        
        #формируем центройды
        X11['centroid']=[0]*len(X11)
        ind_centroid_all=pipe[-1].get_centroid_all_ind(X11)
        X11.loc[ind_centroid_all, 'centroid']=[1]*len(ind_centroid_all)
        self.df_cl['centroid']=X11['centroid'].values
        del X11['centroid'] #удаляем этот столбец, что бы он не учитывался при расчете силуэта
        
        #расчитываем метрики кластеризации по каждому измерению, по каждому кластеру и общую
        
        self.df_cl['silhouette'] = silhouette_samples(X11[cols_in],X11['level_0'] )
        print(silhouette_score(X11[cols_in], X11['level_0']))
        
        c=self.df_cl.groupby(['cl'])['silhouette'].mean()
        c=c.reset_index()
        c.columns=['cl', 'silhouette_cl']
        
        self.df_cl=pd.merge(self.df_cl, c, how='left')
        self.df_cl['silhouette_all']=[self.df_cl['silhouette'].mean()]*len(self.df_cl)
        
        
        #рассчитываем объемы кластеров
        self.df_cl['count']=[1]*len(self.df_cl)
        c=self.df_cl.groupby(['cl'])['count'].sum()
        c=c.reset_index()
        c.columns=['cl', 'cl_num']
        self.df_cl=pd.merge(self.df_cl, c, how='left')
        print('объемы кластеров: \n',c)
        ind=self.get_samples(n_str=20)
        self.df_cl['samples']=[0]*len(self.df_cl)
        self.df_cl.loc[ind, 'samples']=1
        return True
    
    def get_samples(self, n_str=20):
        flag=0
        cols_out='cl'
        
        xx=list(self.df_cl[cols_out].value_counts().index)
        xx.sort()
        flag=0
        for x in xx:
            df2=self.df_cl[self.df_cl[cols_out]==x].sample(n=n_str,  replace=True)
            if flag==0:
                df_rez=df2.copy()
                flag=1
            else:
                df_rez=pd.concat([df_rez, df2], axis=0)
        return list(df_rez.index)
    
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
                """.format(self.Name_table_to) )
            except:
                print("Ошибка очистки таблицы")
                conn.close()
                return False
        try:
            rez=cursor.execute("""
            delete from {0} as tab
            WHERE tab."question_id"={1};
            """.format(self.Name_table_to, self.question_id) )
        except:
            print("Ошибка очистки записей по индексу")
            conn.close()
            return False
        
        try:
            self.execute_values( conn, self.df_cl, self.Name_table_to)
            conn.commit()        
                    
        except:
            print("Ошибка записи данных")
            conn.close()
            return False
        
        conn.close()
        
        print('Сохранение данных завершено {0}'.format(str(datetime.now())))
        return True
    
if __name__=='__main__':
    etl_cl=clastering_free_qwe()
    etl_cl.load_data(question_id=5108)
    etl_cl.transform()
    print(etl_cl.score)
    etl_cl.save_data(add=True)
    
    

       
