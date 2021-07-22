# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 13:54:39 2021

@author: Leonid
"""

import psycopg2
import psycopg2.extras

rez=cursor.execute("""
            SELECT СХЕМА.poll_result_analysis
            id SERIAL NOT NULL,
            student_id integer,
            syllabus varying(512),
            study_group varying(512),
            syllabus_id integer,
            topic_poll_id integer,
            topic_poll_title text,
            topic_poll_is_required boolean,
            topic_poll_was_skipped boolean,
            created_at timestamp without time zone,
            finished_at timestamp without time zone,
            question_id integer 
            question text,
            question_type varying(32),
            answers text,
            student_answers text
            
            """ )