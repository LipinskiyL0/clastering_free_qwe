# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 13:54:39 2021

@author: Leonid
"""

import psycopg2
import psycopg2.extras

rez=cursor.execute("""
            SELECT quiz.poll_result_cl_free_qwe
            
            student_id integer,
            syllabus varying(512),
            study_group varying(512),
            topic_poll_title text,
            
            question_id integer 
            question text,
            student_answers text,
            key_words text,
            cl integer,
            centroid integer,
            'silhouette' float,
            'silhouette_cl' float,
            'silhouette_all' float,
            'count' integer,
            'cl_num' integer,
            'samples' integer
            
            """ )