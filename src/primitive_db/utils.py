# src/primitive_db/utils.py
import json as js

import prompt as pr

from .constants import COMMANDS, TABLE_PATH


# Функция запроса ввода
def user_input() -> str:
  '''
  Функиця запрашивает пользовательский ввод с клавиатуры
  '''
  user_command = pr.string('Введите команду: ')
  
  return user_command.strip()


# Функция загрузки мета данных
def load_metadata(filepath: str) -> dict:
  '''
  filepath - путь до .json файла.
  Функция загружает текущие мета данные из файла
  '''
  try:
    with open(filepath, encoding='utf-8') as metadata_json:
      loaded_metadata = js.load(metadata_json)
    return loaded_metadata
      
  except FileNotFoundError:
    print('Файл c мета данными не найден. Создается новый файл для мета данных.')
    with open(filepath, 'w', encoding='utf-8') as metadata_json:
      metadata_json.write(js.dumps(dict()))
    return {}
    
  except BaseException as error:
    print(f'Произошло исключение: "{error}"')
    return {}
    

def save_metadata(filepath: str, data: dict) -> None:
  '''
  filepath - путь до .json файла,
  data - текущие мета данные.
  Функция выгружает текущие мета данные из программы и записывает их
  в файл для мета данных
  '''
  try:
    saved_metadata = js.dumps(data)
    with open(filepath, 'w', encoding='utf-8') as metadata_json:
      metadata_json.write(saved_metadata)
    return
    
  except (OSError, IOError) as error:
    print(f'Произошла ошибка записи файла: {error}')
    return
    
  except FileNotFoundError:
    print('Файл c мета данными не найден.')
    return
    
  except BaseException as error:
    print(f'Произошло исключение: {error}')
    return

# Функция загрузки данных таблицы из .json файла таблицы
def load_table_data(table_name: str) -> dict:
  '''
  table_name - имя таблицы.
  Функция принимает на вход имя таблицы, и если такая таблица существует,
  то загружает данные таблицы в программу.
  '''
  clean_table_name = table_name.strip().lower()
  try:
    with open(TABLE_PATH / (clean_table_name + '.json'), encoding='utf-8') as tabledata_json: # noqa: E501
      loaded_tabledata = js.load(tabledata_json)
    return loaded_tabledata
    
  except FileNotFoundError:
    print(f'Файл c данными таблицы "{clean_table_name}" не найден.')
    return {}
  
  except BaseException as error:
    print(f'Произошло исключение: "{error}"')
    return {}
    

# Функция сохранения данных таблицы в .json файл
def save_table_data(table_name: str, data: dict) -> None:
  '''
  table_name - имя таблицы,
  data - загружаемые данные таблицы.
  Функция принимаем имя таблицы и загружаемые данные, и если таблица
  существует (существует ее файл), то загружает в него актуальные данные.
  '''
  clean_table_name = table_name.strip().lower()
  try:
    saved_tabledata = js.dumps(data)
    with open(TABLE_PATH / (clean_table_name + '.json'), 'w', encoding='utf-8') as tabledata_json: # noqa: E501
      tabledata_json.write(saved_tabledata)
    return
    
  except FileNotFoundError:
    print(f'Файл c данными таблицы "{clean_table_name}" не найден.')
    return
    
  except BaseException as error:
    print(f'Произошло исключение: "{error}"')
    return


# Функция отображения помощи
def print_help() -> None:
  '''
  Идет по константе с командами и описанием и последовательно выводит
  '''
  for command in COMMANDS.keys():
    print(f'<command> {command} - {COMMANDS[command]}')
    
  return
