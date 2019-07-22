import json
import requests
import math

ApiKey = 'lPpKLU2BhzWU3eB2iWHeZ0XG5fO241JP'
base_url = 'http://forex.1forge.com/1.0.3/'
forex_quote_url = 'http://forex.1forge.com/1.0.3/quotes?pairs='


def main():
    quote_url = forex_quote_url
    response = requests.get(base_url + 'symbols?' + 'api_key=' + ApiKey)
    list_of_symbols = response.json()

    for symbol in list_of_symbols:
        quote_url = quote_url + symbol + ','
    quote_url = quote_url[:-1] + '&api_key=' + ApiKey
    print(quote_url)
    currency_quotes = requests.get(quote_url).json()
    currency_list = []

    #building a graph
    for currency in currency_quotes:
        currency_1 = currency['symbol'][:3]
        currency_2 = currency['symbol'][-3:]
        if currency_1 not in currency_list:
            currency_list.append(currency_1)
        if currency_2 not in currency_list:
            currency_list.append(currency_2)

    print(currency_list)

    number_of_currency = len(currency_list)
    rate_matrix = [[0 for x in range(number_of_currency)] for y in range(number_of_currency)]

    for i in range(number_of_currency):
        for j in range(number_of_currency):
            if i == j:
                rate_matrix[i][j] = 0
            else:
                currency_symbol = currency_list[i] + currency_list[j]
                quote_item = next(item for item in currency_quotes if item["symbol"] == currency_symbol)
                rate_matrix[i][j] = quote_item['price']

    #revert each edge to negative logrithmic
    for i in range(number_of_currency):
        for j in range(number_of_currency):
            if i != j:
                rate_matrix[i][j] = -math.log(rate_matrix[i][j])

if __name__ == '__main__':
	main()
