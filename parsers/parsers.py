#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

actual_dir = os.getcwd()
pathMyModules = actual_dir + '/modules'
sys.path.append(pathMyModules)

import requests
import bs4
import datetime
from sql_main import SqlShell

__all__ = ('aerodar',
           'centravia')  # указать все функции, для использования * в __init__


def aerodar():
    # id агента
    id_carrier = 1

    # log запуска парсера
    log = open(actual_dir + '/log.txt', 'a', encoding='utf-8')
    log.write('\n' + str(datetime.datetime.now()) + ': Запуск парсера aerodar')

    sqlshell = SqlShell()

    try:
        # подсчет количества записей до парсинга
        count_row_before = sqlshell.countInputParsers()

        # пытаемся подключиься и забрать тарифы в html
        try:
            url = 'http://www.aerodar.ru/price/the-prices-of-air-cargo/'
            html = requests.get(url)

            # устранение ошибки конкретно этого парсера
            text_rep = html.text.replace('</tr>\n</tr>', '</tr>')

            soup = bs4.BeautifulSoup(text_rep, 'html.parser')

            # преобразуем данные в нормальный список
            table = []

            for item in soup.find('div', id='table_1').find('tbody').find_all('tr'):
                table.append([x.text.strip() for x in item.find_all('td')])
            table.remove([])

            # поиск и добавление транзитных аэропортов
            for item in table:
                for item2 in item:
                    if int(item2.find('ч/з')) > 0:
                        a = item2.rsplit('ч/з')[1]
                        for char in ') ':
                            a = a.replace(char, '')
                        item.append(a)
                if len(item) < 6:
                    item.append('Отсутствует')
            table.remove(['Отсутствует'])

            # print(len(table))
        except Exception as err:
            print('Ошибка парсинга:', err)
        # print(table)

        # забираем из БД все записи перевозчика
        all_id_by_carrier_before = sqlshell.getAllIdInputParsers(id_carrier)  # table_tmp2
        all_id_by_carrier_after = []
        # print(all_id_by_carrier_before)

        # проверка существования маршрутов и запись/обновление новых
        for route in table:
            city_to, airline_ikao, airport_from_ikao, weight_min_kg, rate_1_rub, city_transit = route
            check_records = sqlshell.checkInputParsers(id_carrier=id_carrier,
                                                       city_to=city_to,
                                                       airline_ikao=airline_ikao,
                                                       airport_from_ikao=airport_from_ikao,
                                                       city_transit=city_transit)
            if len(check_records) == 0:
                insert_records = sqlshell.insertInputParsers(id_carrier=id_carrier,
                                                             city_to=city_to,
                                                             airline_ikao=airline_ikao,
                                                             airport_from_ikao=airport_from_ikao,
                                                             weight_min_kg=weight_min_kg,
                                                             rate_1_rub=rate_1_rub,
                                                             city_transit=city_transit)
                all_id_by_carrier_after.append(insert_records)
            else:
                all_id_by_carrier_after.append(check_records)
                sqlshell.updateInputParsers(id_carrier=id_carrier,
                                            city_to=city_to,
                                            airline_ikao=airline_ikao,
                                            airport_from_ikao=airport_from_ikao,
                                            weight_min_kg=weight_min_kg,
                                            rate_1_rub=rate_1_rub,
                                            city_transit=city_transit)
        # print(len(all_id_by_carrier_after))

        # проверяем устаревшие записи для удаления
        data_old = []
        data_new = []
        for item in all_id_by_carrier_before:
            data_old.append(item[0])
        for item in all_id_by_carrier_after:
            data_new.append(item[0][0])

        data_duplicates = [n for n in data_old if n not in data_new]
        # print(data_duplicates)
        if len(data_duplicates) > 0:
            for id_record_parsers in data_duplicates:
                sqlshell.deleteInputParsers(id_record_parsers)

        log.write('\n' + str(datetime.datetime.now()) + ': Устарело {} записей'.format(len(data_duplicates)))

        # подсчет количества записей после парсинга
        count_row_after = sqlshell.countInputParsers()
        count_row = count_row_after - count_row_before

        # log количество тарифов
        log.write('\n' + str(datetime.datetime.now()) + ': Найдено {} тарифов'.format(len(table)))
        log.write('\n' + str(datetime.datetime.now()) + ': Добавлено {} записей'.format(count_row))
        return count_row_after - count_row_before
    except Exception as err:
        log.write('\n' + str(datetime.datetime.now()) + ': Парсер aerodar завершил работу с ошибкой - {}'.format(err))
    finally:
        # закрываем log
        log.close()
        # закрываем соединение к БД
        sqlshell.closeConnection()


def centravia():
    # id агента
    id_carrier = 3

    # log запуска парсера
    log = open(actual_dir + '/log.txt', 'a', encoding='utf-8')
    log.write('\n' + str(datetime.datetime.now()) + ': Запуск парсера centravia')

    sqlshell = SqlShell()

    try:
        # подсчет количества записей до парсинга
        count_row_before = sqlshell.countInputParsers()

        # пытаемся подключиься и забрать тарифы в html
        try:
            html = requests.get('http://centravia.com/tarify/')

            soup = bs4.BeautifulSoup(html.text, 'html.parser')

            table = []

            for item in soup.find('div', id='content').tbody.find_all('tr'):
                table.append([x.text.strip() for x in item.find_all('td')])

            # поиск и добавление транзитных аэропортов
            for item in table:
                for item2 in item:
                    if int(item2.find('через')) > 0:
                        a = item2.rsplit('через')[1]
                        for char in ') ':
                            a = a.replace(char, '')
                        item.append(a)
                if len(item) < 6:
                    item.append('Отсутствует')

            for item in table:
                if item[4] == '':
                    table.remove(item)
        except Exception as err:
            print('Ошибка парсинга:', err)

        # забираем из БД все записи перевозчика
        all_id_by_carrier_before = sqlshell.getAllIdInputParsers(id_carrier)  # table_tmp2
        all_id_by_carrier_after = []

        # проверка существования маршрутов и запись/обновление новых
        for y in table:
            city_to, weight_min_kg, airline_ikao, rate_1_rub, airport_from_ikao, airport_transit_ikao = y
            check_records = sqlshell.checkInputParsers(id_carrier=id_carrier,
                                                       city_to=city_to,
                                                       airline_ikao=airline_ikao,
                                                       airport_from_ikao=airport_from_ikao,
                                                       airport_transit_ikao=airport_transit_ikao)
            if len(check_records) == 0:
                insert_records = sqlshell.insertInputParsers(id_carrier=id_carrier,
                                                             city_to=city_to,
                                                             airline_ikao=airline_ikao,
                                                             airport_from_ikao=airport_from_ikao,
                                                             weight_min_kg=weight_min_kg,
                                                             rate_1_rub=rate_1_rub,
                                                             airport_transit_ikao=airport_transit_ikao)
                all_id_by_carrier_after.append(insert_records)
            else:
                all_id_by_carrier_after.append(check_records)
                sqlshell.updateInputParsers(id_carrier=id_carrier,
                                            city_to=city_to,
                                            airline_ikao=airline_ikao,
                                            airport_from_ikao=airport_from_ikao,
                                            weight_min_kg=weight_min_kg,
                                            rate_1_rub=rate_1_rub,
                                            airport_transit_ikao=airport_transit_ikao)

        # проверяем устаревшие записи для удаления
        data_old = []
        data_new = []
        for item in all_id_by_carrier_before:
            data_old.append(item[0])
        for item in all_id_by_carrier_after:
            data_new.append(item[0][0])

        data_duplicates = [n for n in data_old if n not in data_new]
        # print(data_duplicates)
        if len(data_duplicates) > 0:
            for id_record_parsers in data_duplicates:
                sqlshell.deleteInputParsers(id_record_parsers)

        log.write('\n' + str(datetime.datetime.now()) + ': Устарело {} записей'.format(len(data_duplicates)))

        # подсчет количества записей после парсинга
        count_row_after = sqlshell.countInputParsers()
        count_row = count_row_after - count_row_before

        # log количество тарифов
        log.write('\n' + str(datetime.datetime.now()) + ': Найдено {} тарифов'.format(len(table)))
        log.write('\n' + str(datetime.datetime.now()) + ': Добавлено {} записей'.format(count_row))
        return count_row_after - count_row_before
    except Exception as err:
        log.write('\n' + str(datetime.datetime.now()) + ': Парсер centravia завершил работу с ошибкой - {}'.format(err))
    finally:
        # закрываем log
        log.close()
        # закрываем соединение к БД
        sqlshell.closeConnection()

