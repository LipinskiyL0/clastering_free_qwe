# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 11:18:45 2021

@author: Leonid
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import datetime 

Name_table={'РПЦТ':'google_analytics.program_rpct', 
            'Регионы':'google_analytics.program_regions',
            'on-line':'google_analytics.program_online'}

Name_table_track={'РПЦТ':'google_analytics.transaction_RPCT',
                  'Регионы':'google_analytics.transaction_regions',
                  'on-line':'google_analytics.transaction_online'
                  
                  }

try:
    conn = psycopg2.connect(dbname='cdto_platform', user='extuser', 
                            password='Lfgg&344h$@&*', host='10.8.1.3', port="5432")
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
except:
    print("Ошибка подключения к базе")
# rez=cursor.execute("""
#            SELECT table_name, column_name 
#            FROM information_schema.columns 
#            WHERE table_schema='public';
#            """ )
# SQL_Query = pd.read_sql_query(
#     '''SELECT table_name, column_name 
#             FROM information_schema.columns 
#             WHERE table_schema='google_analytics'
#     ''',
#     conn)


# df=pd.DataFrame()
# conn.close()

# rez=cursor.execute("""
#            SELECT table_name, column_name 
#            FROM information_schema.columns 
#            WHERE table_schema='public';
#            """ )

df = pd.read_sql_query(
    '''SELECT *
            FROM {0};
    '''.format('quiz.poll_result_analysis'),conn)

# rez=cursor.execute('''DELETE FROM {0}
#                           WHERE date='{1}';
                
#                         '''.format(Name_table['РПЦТ'], str('2021-05-20')))

# df = pd.read_sql_query(
#     '''SELECT *
#             FROM google_analytics.v_primarydata_november_v2
#         WHERE google_analytics.v_primarydata_november_v2.ga_date='{0}';
            
#     '''.format('2020-11-04'),conn)


# df = pd.read_sql_query('РПЦТ'
#     '''SELECT *
#             FROM google_analytics.v_primarydata_ext 
#        WHERE (google_analytics.v_primarydata_ext.view_name='{0}' )AND 
#              (google_analytics.v_primarydata_ext.ga_timestamp BETWEEN '{1}' AND '{2}')
       
#        --LIMIT 10000
            
#     ;'''.format('РПЦТ', '2021-03-10', '2021-03-31 00:00:00+0000'),conn)

# df1=pd.read_sql_query(
#     '''SELECT MAX(date)
#             FROM {0}
            
#     '''.format(Name_table['РПЦТ']),conn)

# rez=cursor.execute('''DELETE FROM google_analytics.program_rpct
#                      WHERE (google_analytics.program_rpct.date BETWEEN '{1}' AND '{2}');
            
#                     '''.format(Name_table['РПЦТ'], str('2021-05-20'), str('2021-06-08')))
# conn.commit()  

# df = pd.read_sql_query('''SELECT *
#                                             FROM google_analytics.program_rpct
#                                         WHERE (google_analytics.program_rpct.date BETWEEN '{1}' AND '{2}')
                                            
#                                     ;'''.format('РПЦТ', str('2021-05-18'), str('2021-06-08')),conn)
                    
conn.close()