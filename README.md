# clastering_free_qwe

В проекте реализована загрузка и кластеризация текстовых ответов на свободные вопросы. 
Общая схема работы следующая: 
Шаг 1. Происходит загрузка текстовых ответов с помощью объекта load_transform_free_qwe описанного в файле load_transform_free_qwe.py
       Схема загркузки: etl_qwe=load_transform_free_qwe() - создаем объект класса
        		etl_qwe.load_data(date=['2021-03-01', '2021-04-01']) - загружаем в указанные данные все вопросы и иответы
       			etl_qwe.transform() - преобразуем данные. извлекаем ответы
        		etl_qwe.save_data(add=False) - сохраняем данные в DWH
			В результате отработки этой схемы текстовые ответы выкачиваются из платформы и сохраняются в таблицу 
			'quiz.poll_result_analysis'

Шаг 2. Происходит кластеризация с помощью объекта clastering_free_qwe описанного в файле clastering_free_qwe.py. 
	Схема работы:
			etl_cl=clastering_free_qwe() - создаем объект класса.
 
    			etl_cl.load_data(question_id=5721) - выгружаем из таблицы 'quiz.poll_result_analysis' все текстовые ответы по заданному question_id
							     в данном случе question_id=5721
    			etl_cl.transform() - происходит сама кластеризация
    			print(etl_cl.score) - оценка точности
    			etl_cl.save_data(add=False) - результат кластеризации выводится в таблицу 'quiz.poll_result_cl_free_qwe'
						      с дозаписью (add=True), или перезаписью (add=False) 
	