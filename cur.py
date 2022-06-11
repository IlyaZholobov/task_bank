from dataclasses import dataclass
from sys import argv
import re
import sqlite3
from zeep import Client
from datetime import datetime

URL = 'https://cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL'

REXEXP_DATE = "([0-3][0-9]\.(1[0-2]|0[1-9])\.[0-9]{4})"

REXEXP_CUR_CODES = "(([0-9]{1,}){1,}(,|)){1,}"

DB = 'curdb.db'

CURRENCY_ORDER_TABLE = 'CURRENCY_ORDER'

CURRENCY_RATES_TABLE = 'CURRENCY_RATES'

ENCODING = 'utf-8'

ondate = None

cur_code_list = None

currency_list = []

order = None

cursor = None


@dataclass()
class Order:
    id: int
    ondate: str


@dataclass()
class Currency:

    def __init__(self, cur_dic_xml: dict = {}, order_id: int = 0):
        if cur_dic_xml and order_id:
            for cur_dic_xml_key in self.dic_xml_assoc:
                self.__setattr__(self.dic_xml_assoc[cur_dic_xml_key],
                                 cur_dic_xml[cur_dic_xml_key])

            self.scale = int(self.scale)
            self.order_id = order_id

    order_id: int
    name: str
    numeric_code: str
    alphabetic_code: str
    scale: int
    rate: str
    dic_xml_assoc = {
        'Vname': 'name',
        'Vcode': 'numeric_code',
        'VchCode': 'alphabetic_code',
        'Vnom': 'scale',
        'Vcurs': 'rate'
    }


def logger(text: str = None, is_end: bool = True):

    global cursor

    t = datetime.now()

    exit_fl = False

    if text is None:
        if is_end is True:
            exit_fl = True
        text = 'завершение работы' if is_end else 'начало работы'

    print(text, f'datetime {t}')

    filename = argv[0].replace('.py', '.log')
    with open(filename, 'a', encoding=ENCODING) as f:
        f.write(f'{text} datetime {t}\n')

        if exit_fl:
            f.write('==' * 10 + '\n')
            cursor.close()
            exit()


def db_connect():
    global cursor

    try:

        if cursor is None:

            sqlite_connection = sqlite3.connect(DB)
            cursor = sqlite_connection.cursor()
            logger("Успешное подключение к базе SQLite")

        return cursor

    except sqlite3.Error as error:

        logger(f"Ошибка при подключении к sqlite {error}")

        logger()


def check_input_date(date: str):

    global ondate

    pattern = re.compile(REXEXP_DATE)

    _ondate = pattern.match(date)

    if _ondate:
        ondate = _ondate.group(0)
        logger(f'дата установки курсов {ondate}')
    else:
        logger('некорректная дата')
        logger()


def check_input_cur_list(cur_sring: str):

    global cur_code_list

    pattern = re.compile(REXEXP_CUR_CODES)

    _cur_list = pattern.match(cur_sring.replace(' ', ''))

    if _cur_list:
        cur_code_list = [x for x in _cur_list.group(0).split(',')]

        cur_code_list_str = ','.join(cur_code_list)

        logger(f'список цифровых кодов {cur_code_list_str}')
    else:
        logger('некорректно указан список цифровых кодов')
        logger()


def get_input_value():

    date = input('Введите дату в формате %d.%m.%Y ')
    cur_string = input('Введите список цифровых кодов валют через запятую ')

    check_input_date(date)
    check_input_cur_list(cur_string)


def check_new_order():
    global ondate
    global cur_code_list

    cur_list_str_query = '","'.join(cur_code_list)

    cur = db_connect()

    check_query = f'''
    SELECT numeric_code
    FROM CURRENCY_ORDER
    LEFT JOIN CURRENCY_RATES ON id = order_id
    WHERE ondate = '{ondate}' AND numeric_code in ("{cur_list_str_query}")
    '''

    res = [x[0] for x in cur.execute(check_query).fetchall()]

    if len(res) == len(cur_code_list) or len(res) == len((set(cur_code_list))):

        logger(
            'Такое распоряжение уже существует для указанных цифровых кодов')

        logger()

    if len(res):
        cur_code_list = [
            cur_code for cur_code in cur_code_list if cur_code not in res
        ]

    is_exist_query = f'''
    SELECT id
    FROM CURRENCY_ORDER
    WHERE ondate = '{ondate}' '''

    res = cur.execute(is_exist_query).fetchone()

    if res:
        logger(
            'распоряжение существует, но не существуют некоторые заданные курсы валют согласно цифровым кодам'
        )
        return res[0]


def create_oder():
    global ondate

    global order

    db_ord_id = check_new_order()

    if db_ord_id is None:

        cur = db_connect()

        new_order_query = f'''
        INSERT INTO {CURRENCY_ORDER_TABLE} (ondate)
        VALUES ('{ondate}')'''

        cur.execute(new_order_query).connection.commit()

        logger('распоряжение создано')

    order = Order(db_ord_id if db_ord_id is not None else cur.lastrowid,
                  ondate)

    logger(order)


def parse_currency_xml(cur_xml):
    d = {}
    for attrib in cur_xml:
        d[attrib.tag] = attrib.text.strip()

    return d


def get_currencies():

    global order

    global currency_list

    logger('получение курсов валют')

    client = Client(URL)

    result = client.service.GetCursOnDateXML(
        datetime.strptime(order.ondate, '%d.%m.%Y')).iter('ValuteCursOnDate')

    logger('обработка курсов валют')

    for obj in result:
        currency_list.append(Currency(parse_currency_xml(obj), order.id))


def build_insert_cur_query(cur: Currency):

    fields = []
    values = ''

    for field in cur.__dict__:
        fields.append(field)
        if isinstance(cur.__getattribute__(field), int):
            values += f'{cur.__getattribute__(field)}'
        else:
            values += f"'{cur.__getattribute__(field)}'"
        if len(fields) <= len(cur.__dict__) - 1:
            values += ', '

    fields = ','.join(fields)

    return f"INSERT INTO CURRENCY_RATES ({fields}) VALUES ({values})"


def insert_currencies():
    global currency_list
    global cur_code_list

    cur = db_connect()

    logger('запись курсов валют в бд')
    for currency in currency_list:

        if currency.numeric_code in cur_code_list:
            logger(currency)
            insert_cur_query = build_insert_cur_query(currency)

            cur.execute(insert_cur_query).connection.commit()


def currency_row_factory(cursor, row):

    currency = Currency()

    for idx, col in enumerate(cursor.description):
        currency.__setattr__(col[0], row[idx])

    return currency


def get_loadad_currencies_db():

    global cur_code_list
    global order

    cur = db_connect()

    cur.row_factory = currency_row_factory

    cur_list_str_query = '","'.join(cur_code_list)

    select_loaded_currencies_query = f'''
    SELECT *
    FROM CURRENCY_RATES
    WHERE order_id = '{order.id}' AND numeric_code in ("{cur_list_str_query}")'''

    res = cur.execute(select_loaded_currencies_query).fetchall()

    return res


def print_loaded_currency():
    global order

    loaded_currency_list = get_loadad_currencies_db()

    logger('загруженные курсы')

    logger(
        f'номер распоряжения {order.id} дата установки курсов {order.ondate}')
    for currency in loaded_currency_list:
        logger(
            f'\tвалюта {currency.name} номинал {currency.scale} курс {currency.rate}'
        )


def create_currencies():

    get_currencies()

    insert_currencies()


def main():

    get_input_value()

    create_oder()

    create_currencies()

    print_loaded_currency()


if __name__ == '__main__':

    logger(is_end=False)

    main()

    logger()
