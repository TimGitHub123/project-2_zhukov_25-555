# src/primitive_db/engine.py
import shlex as sh

from .constants import META_FILEPATH
from .core import (
  create_table,
  delete,
  drop_table,
  info,
  insert,
  list_tables,
  select,
  update,
)
from .parser import parser_clause
from .utils import (
  load_metadata,
  load_table_data,
  print_help,
  save_metadata,
  save_table_data,
  user_input,
)


def run():
  print('***')
  print_help()
  print('\n')
  
  while True:
    print('\n')
    current_metadata = load_metadata(filepath=META_FILEPATH)
    user_cmd = user_input()
    lexer = sh.shlex(user_cmd, posix=False)
    lexer.whitespace_split = True
    args = list(lexer)
    
    match args[0].lower():
    
      case 'create_table':
        if len(args) < 3:
          print('Неправильный формат ввода команды.')
          continue
        current_metadata = create_table(
          metadata=current_metadata,
          table_name=args[1],
          columns=args[2:]
        )
        save_metadata(filepath=META_FILEPATH, data=current_metadata)
      
      case 'drop_table':
        if len(args) < 2:
          print('Неправильный формат ввода команды.')
          continue
          
        current_metadata = drop_table(metadata=current_metadata, table_name=args[1])
        save_metadata(filepath=META_FILEPATH, data=current_metadata)
        
      case 'list_tables':
        list_tables(metadata=current_metadata)
        
      case 'insert':
        if len(args) < 5:
          print('Неправильный формат ввода команды.')
          continue
          
        if args[1].lower() == 'into' and args[3].lower() == 'values':
          table_name = args[2]
          table_name_clean = table_name.strip().lower()
          current_table_data = load_table_data(table_name=table_name_clean)
          
          if not current_table_data:
            print("Ошибка. Функция load_table_data не смогла получить данные таблицы.")
            continue
            
          current_table_data = insert(metadata=current_metadata, data=current_table_data, table_name=table_name_clean, values=' '.join(args[4:])) # noqa: E501
          save_table_data(table_name=table_name_clean, data=current_table_data)
          
        else:
          print('Неправильный формат ввода ключевых слов.')

      case 'select':
        if len(args) < 3:
          print('Неправильный формат ввода команды.')
          continue
          
        if args[1].lower() == 'from':
          table_name = args[2]
          table_name_clean = table_name.strip().lower()
          current_table_data = load_table_data(table_name=table_name_clean)
          print(current_table_data)
          if not current_table_data:
            print('Ошибка. Функция load_table_data не смогла получить данные таблицы.')
            continue
            
          if args[3:]:
            where_clause = parser_clause(clause=args[3:])
            if not where_clause:
              continue
            select(table_name=table_name_clean, table_data=current_table_data, metadata=current_metadata, where_clause=where_clause) # noqa: E501
            
          else:
            select(table_name=table_name_clean, table_data=current_table_data)
          
        else:
          print('Неправильный формат ввода ключевых слов.')
    
      case 'update':
        if len(args) != 10:
          print('Неправильный формат ввода команды.')
          continue
        if args[2].lower() == 'set' and args[6].lower() == 'where' and args[4] == '=' and args[8] == '=': # noqa: E501
          table_name = args[1]
          table_name_clean = table_name.strip().lower()
          current_table_data = load_table_data(table_name=table_name_clean)
          
          if not current_table_data:
            print('Ошибка. Функция load_table_data не смогла получить данные таблицы.')
            continue
          
          set_clause = parser_clause(clause=args[2:6])
          if not set_clause:
            continue
            
          where_clause = parser_clause(clause=args[6:])
          if not where_clause:
            continue
          updated_table_data = update(table_name=table_name_clean, metadata=current_metadata, table_data=current_table_data, set_clause=set_clause, where_clause=where_clause) # noqa: E501
          
          save_table_data(table_name=table_name_clean, data=updated_table_data)
        else:
          print('Неправильный формат команды.')
          
      case 'delete':
        if len(args) != 7:
          print('Неправильный формат ввода команды.')
          continue
        if args[1].lower() == 'from' and args[3].lower() == 'where' and args[5] == '=':
          table_name = args[2]
          table_name_clean = table_name.strip().lower()
          current_table_data = load_table_data(table_name=table_name_clean)
          print(current_table_data)
          if not current_table_data:
            print('Ошибка. Функция load_table_data не смогла получить данные таблицы.')
            continue
            
          where_clause = parser_clause(clause=args[3:])
          if not where_clause:
            continue
          updated_table_data = delete(table_name=table_name_clean, metadata=current_metadata, table_data=current_table_data, where_clause=where_clause) # noqa: E501
          
          save_table_data(table_name=table_name_clean, data=updated_table_data)
        else:
          print('Неправильный формат команды.')
      
      case 'info':
        if len(args) != 2:
          print('Неправильный формат ввода команды.')
          continue
        table_name = args[1]
        table_name_clean = table_name.strip().lower()
        current_table_data = load_table_data(table_name=table_name_clean)
          
        if not current_table_data:
          print('Ошибка. Функция load_table_data не смогла получить данные таблицы.')
          continue
        info(table_name=table_name_clean, metadata=current_metadata, table_data=current_table_data) # noqa: E501
      
      case 'exit':
        print('Программа остановлена!')
        break
      
      case 'help':
        print_help()
        
      case _:
        print(f'Функции {args[0]} нет в программе. Попробуйте снова.')
  
