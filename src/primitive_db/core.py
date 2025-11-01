# src/primitive_db/core.py
import ast
import json as js
import os
import re

import prettytable as pt

from .constants import CURRENT_TYPES, TABLE_PATH


# Функция создания таблицы
def create_table(metadata: dict, table_name: str, columns: list) -> dict:
  '''
  metadata - текущая мета дата,
  table_name - название новой таблицы,
  columns - список полей таблицы.
  Функция проверяет, что существует таблица с таким названием и что формат
  столбцов соответствующий. Так же проверяет, что типы заданы верно. Если все
  нормально, то добавляет такую новую таблицу в текущие мета данные.
  '''
  try:
    # Очистка
    table_name_clean = table_name.strip().lower()
    columns_clean = [name_value.strip().lower() for name_value in columns]
    
    if table_name_clean not in metadata.keys():
      pattern = r'^[^:]+:[^:]+$'
      
      for name_value in columns_clean:
        if re.match(pattern, name_value):
          if name_value.split(':')[1] in CURRENT_TYPES:
            continue
          else:
            print(f'Некорректный тип поля: "{name_value.split(':')[1]}". Попробуйте снова.') # noqa: E501
            return metadata
        else:
          print(f'Некорректное значение: "{name_value}". Попробуйте снова.')
          return metadata
      
      if len(set([name_value.split(':')[0] for name_value in columns_clean])) != len(columns_clean): # noqa: E501
        print('Названия столбцов должны быть уникальные.')
        return metadata
      
      if columns_clean[0] != 'id:int':
        if 'id:int' not in columns_clean:
          columns_clean = ['id:int'] + columns_clean
        else:
          columns_clean.remove('id:int')
          columns_clean = ['id:int'] + columns_clean
      
      dict_columns = {}
      for name_value in columns_clean:
        name, value = name_value.split(':')
        dict_columns[name] = value
        
      metadata[table_name_clean] = dict_columns
      
      table_data = {}
      for column_name_type in columns_clean:
        table_data[column_name_type.split(':')[0]] = []
      table_data_json = js.dumps(table_data, ensure_ascii=False)
      
      with open(TABLE_PATH / (table_name_clean + '.json'), 'w', encoding='utf-8') as new_data_file: # noqa: E501
        new_data_file.write(table_data_json)
      
      print(f'Таблица "{table_name_clean}" успешно создана со столбцами: {", ".join(columns_clean)}') # noqa: E501
      return metadata

    print(f'Ошибка: Таблица "{table_name_clean}" уже существует.')
    return metadata
    
  except ValueError as error:
    print(f'Некорректное значение: "{error}". Попробуйте снова.')
    return metadata
    
  except BaseException as error:
    print(f'Произошло исключение: "{error}"')
    return metadata

  
# Функция удаления таблицы
def drop_table(metadata: dict, table_name: str) -> dict:
  '''
  metadata - текущая мета дата,
  table_name - название таблицы для удаления.
  Функция получает мета данные и название таблицы для удаления на вход,
  и если такая таблица существует, то удаляет ее, иначе выводит ошибку.
  '''
  table_name_clean = table_name.strip().lower()
  
  try:
    if table_name_clean in metadata.keys():
      table_current_path = TABLE_PATH / (table_name_clean + '.json')
      if os.path.exists(table_current_path):
        os.remove(table_current_path)
        print(f'Таблица "{table_name_clean}" успешно удалена.')
        
      else:
        print('Файл таблицы не найден.')
        
      del metadata[table_name_clean]
      return metadata
    
    print(f'Ошибка: Таблица "{table_name_clean}" не существует.')
    return metadata
  
  except ValueError as error:
    print(f'Некорректное значение: "{error}". Попробуйте снова.')
    return metadata
    
  except BaseException as error:
    print(f'Произошло исключение: "{error}"')
    return metadata
    

# Функция вывода всех текущих таблиц в БД
def list_tables(metadata: dict) -> None:
   '''
   Выводит с новой строки имена всех таблиц в бд
   '''
   if len(metadata.keys()) == 0:
     print('База данных пустая.')
     return
     
   for table_name in metadata.keys():
     print(f'- {table_name}')
   
   return
  
  
# Функция вставки данных в таблицу
def insert(metadata: dict, data: dict, table_name: str, values: str) -> dict:
  '''
  metadata - текущие метаданные БД,
  data - данные таблицы,
  table_name - имя таблицы,
  values - значения для вставки.
  Функция принимает текущие данные и вставляет данные в таблицу, если типы
  данных соответствуют схеме таблицы.
  '''
  table_name_clean = table_name.strip().lower()
  if table_name_clean not in metadata.keys():
    print('Таблицы с указанным именем не существует.')
    return data
  
  values_clean = values.replace('true', 'True').replace('false', 'False')

  values_clean = ast.literal_eval(values_clean)
  if not isinstance(values_clean, tuple):
    raise TypeError('Неправильный формат ввода значений. Попробуйте снова.')
    return data
  
  if len(metadata[table_name_clean]) - 1 != len(values_clean):
    if len(values) > len(metadata.keys()) - 1:
      print('Слишком много элементов для вставки.')
      return data
    else:
      print('Слишком мало элементов для вставки.')
      return data
  
  for idx, value in enumerate(values_clean):
    value_type_name = type(value).__name__
    if value_type_name.lower() not in CURRENT_TYPES:
      print(f'Недопустимый тип элемента: {value_type_name}. Такой тип не поддерживается.') # noqa: E501
      return data
    scheme_type = metadata[table_name_clean][list(metadata[table_name_clean].keys())[idx + 1]] # noqa: E501
    if value_type_name != scheme_type: 
      print(f'Тип данных элемента {value} не соответствует схеме таблицы. Измените тип {value_type_name} на {scheme_type}.') # noqa: E501
      return data
      
  max_id = max([0] if not data['id'] else data['id'])
  data['id'].append(max_id + 1)
  
  for i in range(len(data.keys())):
    if i == 0:
      continue
    data[list(data.keys())[i]].append(values_clean[i - 1])
    
  print(f'Запись с ID={max_id + 1} успешно добавлена в таблицу {table_name_clean}')
  return data
  

# Функция select таблицы
def select(table_name: str, table_data: dict, metadata: dict = None, where_clause: dict = None) -> None: # noqa: E501
  '''
  table_name - имя таблицы,
  table_data - данные таблицы,
  metadata - текущие мета данные,
  where_clause - условие where.
  Функция select, выводит строчки, которые соответствуют условию where,
  если оно задано. Если нет, то выводит все строки таблицы.
  '''
  pretty_table = pt.PrettyTable()
  try:
    if where_clause is not None:
      where_column = list(where_clause.keys())[0]
      where_value = list(where_clause.values())[0]
      
      if where_column in list(table_data.keys()):
        if metadata[table_name][where_column] != type(where_value).__name__:
          print(f'Тип данных в условии where/set: {str(type(where_value).__name__)} не совпадает с типом данных {metadata[table_name][where_column]} в схеме таблицы.') # noqa: E501
          return

        indexes_to_print = find_indices(column_values=table_data[where_column], value=where_value) # noqa: E501

        if len(indexes_to_print) == 0:
            pretty_table.field_names = table_data.keys()
            print(pretty_table)
            return

        indexes_to_print.sort()
        valuse_to_print = {}

        for name in table_data.keys():
          new_values = []
          for ind in indexes_to_print:
            new_values.append(table_data[name][ind])
          valuse_to_print[name] = new_values

        pretty_table.add_rows(list(zip(*valuse_to_print.values())))
        print(pretty_table)
        return
        
      else:
        print(f'Столбца {where_column} нет в таблице {table_name}.')
        return
    pretty_table.field_names = table_data.keys()
    pretty_table.add_rows(list(zip(*table_data.values())))
    print(pretty_table)
    return
    
  except BaseException as error:
    print(f'Произошла ошибка: {error}.')
    return

  
# Функция обновления данных в таблице
def update(table_name: str, metadata: dict, table_data: dict, set_clause: dict, where_clause: dict) -> dict: # noqa: E501
  '''
  table_name - имя таблицы,
  metadata - текущие мета данные,
  table_data - данные таблицы,
  set_clause - условие set для вставки.
  where_clause - условие where.
  Функция update, обновляет значение по условию where, если
  такая запись находится.
  '''
  try:
    where_column = list(where_clause.keys())[0]
    where_value = list(where_clause.values())[0]
    
    set_column = list(set_clause.keys())[0]
    set_value = list(set_clause.values())[0]
    
    if where_column in list(table_data.keys()) and set_column in list(table_data.keys()): # noqa: E501
      if metadata[table_name][where_column] != type(where_value).__name__ or metadata[table_name][set_column] != type(set_value).__name__: # noqa: E501
        print(f'Тип данных в условии where/set: {type(where_value).__name__}/{type(set_value).__name__:} не совпадает с типом данных {metadata[table_name][where_column]}/{metadata[table_name][set_column]} в схеме таблицы.') # noqa: E501
        return table_data

      where_to_update = find_indices(column_values=table_data[where_column], value=where_value) # noqa: E501

      if len(where_to_update) == 0:
        print('Условие where не нашло ни одной записи, таблица не была изменена.')
        return table_data

      elif len(where_to_update) > 1:
        print(f'Найдено {len(where_to_update)} совпадений в таблице по условию where. Будет изменено первое по порядку значение.') # noqa: E501
      
      where_to_update = where_to_update[0]
      table_data[set_column][where_to_update] = set_value
      print(f'Запись с ID={table_data['id'][where_to_update]} в таблице {table_name} успешно обновлена.') # noqa: E501
        
      return table_data
        
    else:
      print(f'Столбца {where_column} или {set_column} нет в таблице {table_name}.')
      return table_data
    
  except BaseException as error:
    print(f'Произошла ошибка: {error}.')
    return table_data
    
    
# Функция удаления записей из таблицы
def delete(table_name: str, metadata: dict, table_data: dict, where_clause: dict) -> dict: # noqa: E501
  '''
  table_name - имя таблицы,
  table_data - данные таблицы,
  metadata - текущие мета данные,
  where_clause - условие where.
  Функция delete, удаляет запись, которая соответствует условию where.
  '''

  try:
    where_column = list(where_clause.keys())[0]
    where_value = list(where_clause.values())[0]
    
    if where_column in list(table_data.keys()):
      if metadata[table_name][where_column] != type(where_value).__name__:
        print(f'Тип данных в условии where: {type(where_value).__name__} не совпадает с типом данных {metadata[table_name][where_column]} в схеме таблицы.') # noqa: E501
        return table_data

      where_to_delete = find_indices(column_values=table_data[where_column], value=where_value) # noqa: E501

      if len(where_to_delete) == 0:
        print('Условие where не нашло ни одной записи, таблица не была изменена.')
        return table_data

      elif len(where_to_delete) > 1:
        print(f'Найдено {len(where_to_delete)} совпадений в таблице по условию where. Будут удалены все эти записи.') # noqa: E501
      where_to_delete.sort(reverse=True)
      print(table_data.keys())
      
      for ind in where_to_delete:
        print(f'Запись с ID={table_data['id'][ind]} в таблице {table_name} успешно удалена.') # noqa: E501
        for name in table_data.keys():
          del table_data[name][ind]
      return table_data
        
    else:
      print(f'Столбца {where_column} нет в таблице {table_name}.')
      return table_data
    
  except BaseException as error:
    print(f'Произошла ошибка: {error}.')
    return table_data
    
    
# Функция для выведения схемы таблицы
def info(table_name: str, metadata: dict, table_data: dict) -> None:
  '''
  table_name - название таблицы,
  metadata - мате данные,
  table_data - данные из таблицы.
  Функция выводит основую информацию о таблице.
  '''
  print(f'Таблица: {table_name}')
  print(f'Столбцы: {", ".join([list(metadata[table_name].keys())[i] + ":" + list(metadata[table_name][list(metadata[table_name].keys())[i]].keys())[0] for i in range (len(metadata[table_name].keys()))])}') # noqa: E501
  print(f'Количество записей: {len(table_data[list(table_data.keys())[0]])}')
  return
    
    
# Вспомогательная функция для поиска значений
def find_indices(column_values: list, value) -> list:
  '''
  column_values - список значений столбца,
  value - значение для поиска.
  Вспомогательная функция для получения всех индексов элементов,
  которые равны заданному
  '''
  indices = []
  for i, elem in enumerate(column_values):
    if elem == value:
      indices.append(i)
      
  return indices
