#!/usr/bin/env python3
import requests
import pandas as pd
from bs4 import BeautifulSoup

import sys
import json
import time
from os import listdir
from datetime import date
from datetime import datetime

today = datetime.today()

# dd/mm/YY
scraped_date = today
# print(scraped_date)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}

def clean_numbers(text:str) -> str:
    return text.replace("-", "0").replace(".", "").replace(",", ".")


def extract_data_from(table:str, position:int) -> str:
    try:
        return table.select('.data .txt')[position].string.strip()
    except:
        return ''


def process_data(stock:str, tables:list, is_bank:bool=False) -> dict:
    data = {
       'scraped_date': str(scraped_date),
       # 0
       'codigo': stock,
       'cotacao': float(clean_numbers(extract_data_from(tables[0], 1))),
       'data_cotacao': str(datetime.strptime(extract_data_from(tables[0], 3), '%d/%m/%Y')),
       'setor': extract_data_from(tables[0], 6),
       'subsetor': extract_data_from(tables[0], 8),
       # 1 - Valor de mercado
       'valor_mercado': extract_data_from(tables[1], 0).replace(".", ""),
       'valor_firma': extract_data_from(tables[1], 2).replace(".", ""),
       'numero_acoes': extract_data_from(tables[1], 3).replace(".", ""),
       # 2 - Indicadores fundamentalistas
       'pl': float(clean_numbers(extract_data_from(tables[2], 0))),
       'lpa': float(clean_numbers(extract_data_from(tables[2], 1))),
       'pvp': float(clean_numbers(extract_data_from(tables[2], 2))),
       'vpa': float(clean_numbers(extract_data_from(tables[2], 3))),
       'pebit': float(clean_numbers(extract_data_from(tables[2], 4))),
       'marg_bruta': float(clean_numbers(extract_data_from(tables[2], 5)).replace("%", ""))/100,
       'psr': float(clean_numbers(extract_data_from(tables[2], 6))),
       'marg_ebit': float(clean_numbers(extract_data_from(tables[2], 7)).replace("%", ""))/100,
       'pativos': float(clean_numbers(extract_data_from(tables[2], 8))),
       'marg_liquida': float(clean_numbers(extract_data_from(tables[2], 9)).replace("%", ""))/100,
       'p_cap_giro': float(clean_numbers(extract_data_from(tables[2], 10))),
       'ebit_ativo': float(extract_data_from(tables[2], 11).replace(".", "").replace("-", "0").replace(",", ".").replace("%", ""))/100,
       'p_ativ_circ_liq': float(clean_numbers(extract_data_from(tables[2], 12))),
       'roic': float(extract_data_from(tables[2], 13).replace(".", "").replace("-", "0").replace(",", ".").replace("%", ""))/100,
       'div_yield': float(extract_data_from(tables[2], 14).replace(".", "").replace("-", "0").replace(",", ".").replace("%", ""))/100,
       'roe': float(extract_data_from(tables[2], 15).replace(".", "").replace("-", "0").replace(",", ".").replace("%", ""))/100,
       'ev_ebitida': float(clean_numbers(extract_data_from(tables[2], 16))),
       'liquidez_corr': float(clean_numbers(extract_data_from(tables[2], 17))),
       'giro_ativos': float(clean_numbers(extract_data_from(tables[2], 18))),
       'div_br_patrim': float(clean_numbers(extract_data_from(tables[2], 19))),
       'cres_rec_5a': float(extract_data_from(tables[2], 20).replace(".", "").replace("-", "0").replace(",", ".").replace("%", ""))/100,
       
       # 3 - Balanço patrimonial
       'ativo': float(clean_numbers(extract_data_from(tables[3], 0))),
       'disponibilidades': float(clean_numbers(extract_data_from(tables[3], 2))),
       'ativo_circulante':  " 0.0 " if is_bank else float(clean_numbers(extract_data_from(tables[3], 4))),
       'div_bruta': " 0.0 "  if is_bank else float(clean_numbers(extract_data_from(tables[3], 1))),
       'div_liquida': " 0.0 " if is_bank else  float(clean_numbers(extract_data_from(tables[3], 3))),
       'patrimonio_liquido': float(clean_numbers(extract_data_from(tables[3], 3))),
       
       # 4 - Demonstrativo de resultados
       'receita_Liquida_12': float(clean_numbers(extract_data_from(tables[4], 0))),
       'receita_Liquida_3': float(clean_numbers(extract_data_from(tables[4], 1))),
       'ebit_12': float(clean_numbers(extract_data_from(tables[4], 2))),
       'ebit_3': float(clean_numbers(extract_data_from(tables[4], 3))),
       'lucro_liquido_12': float(clean_numbers(extract_data_from(tables[4], 2))),
       'lucro_liquido_3': float(clean_numbers(extract_data_from(tables[4], 3))),

       }
    
    return data 


def get_data() -> None:

    stocks = []
    if "tickers.txt" in listdir():
        with open("tickers.txt", "r") as fundamentus_file:
            stocks = fundamentus_file.read().split()
    
    if len(stocks) <= 0:
        stocks = ['BBAS3', 'PETR4', 'PETR3', 'WEG3', 'OPCT3', 'UGPA3', 'RRRP3', 'CSAN3', 'RAIZ4', 'VBBR3', 'PRIO3']

    print(stocks)
    stocks_info = []
    for stock in stocks:
        try:
            print(stock)
            
            stock_url = f"https://fundamentus.com.br/detalhes.php?papel={stock}"
            
            page = requests.get(stock_url, headers=headers)
            
            html = BeautifulSoup(page.text, 'html.parser')
            #print(html)

            # Tabelas
            # 0 - Cotação
            tables = html.select("table.w728")
            
            if len(tables) == 0:
                continue
            resp = extract_data_from(tables[0], 8)
            if resp is None:
                continue

            if extract_data_from(tables[0], 8) == "Bancos":
                is_bank = True
            else:
                is_bank = False

            stocks_info.append(process_data(stock=stock, tables=tables, is_bank=is_bank))

        except Exception as ex: print(ex, sys.exc_info()[2].tb_lineno)
    
    df = pd.DataFrame(stocks_info)

    df.to_csv(f'extraction_{today.strftime("%Y_%m_%d")}.txt', sep='|', encoding='latin1')
    print(df)



if __name__ == '__main__':
    get_data()