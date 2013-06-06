import sys
import csv
import urllib.request
import datetime
import sqlite3


prefix = "S_"
tdate = 'date'
name = 'name'
currency = 'currency'
high = 'high'
low = 'low'
close = 'close'
volume = 'volume'
openp = 'open'
symbol = 'symbol'

def download(datestr, outfile):
    sgx_url = ("http://infopub.sgx.com/Apps?A=COW_Prices_Content&B=SecuritiesHistoricalPrice&F=3949&G=SESprice.dat&H=" + 
               datestr)
    out_file = open(outfile, "wb")
    data = urllib.request.urlopen(sgx_url).read()
    out_file.write(data)
    out_file.close()


def get_data(row):
    datarow = {}
    dt = datetime.datetime.strptime(row[0], '%Y-%m-%d')
    datarow[tdate] = datetime.date(dt.year, dt.month, dt.day)
    datarow[name] = row[1].strip()
    datarow[currency] = row[3].strip()
    datarow[high] = float(row[4])
    datarow[low] = float(row[5])
    datarow[close] = float(row[6])
    datarow[volume] = int(row[8])
    datarow[openp] = float(row[12])
    datarow[symbol] = row[14].rstrip()
    if datarow[volume] == 0:
        datarow[high] = datarow[close]
        datarow[low] = datarow[close]
        datarow[openp] = datarow[close]
    return datarow


def prepare_data(conn, data):
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO stocks VALUES
    (?, ?, ?)''', (data[symbol], data[name], data[currency]))
    c.execute('CREATE TABLE IF NOT EXISTS ' +
              prefix + data[symbol] +
              '''(tdate date, open real, high real, low real,
               close real, volume integer)''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS ' +
              prefix + data[symbol] + '_date_ind' +
              ' on ' +
              prefix + data[symbol] +
              '(tdate)')
    conn.commit()
    c.close()


def import_data(conn, data):
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO ' +
              prefix + data[symbol] +
              ' VALUES(?, ?, ?, ?, ?, ?)',
              (data[tdate], data[openp],
               data[high], data[low],
               data[close], data[volume]))
    conn.commit()
    c.close()


def import_row(conn, row):
    #print('imp', row)
    data = get_data(row)
    print('imp', data[symbol], data[tdate])
    prepare_data(conn, data)
    import_data(conn, data)


def import1(conn, datestr):
    outfile = "temp.dat"
    download(datestr, outfile)
    with open(outfile, newline='') as datafile:
        reader = csv.reader(datafile, delimiter=';')
        for row in reader:
            if len(row) == 16:
                import_row(conn, row)
            else:
                pass


def import_historical(conn, **kwargs):
    datefmt = '%Y-%m-%d'
    today = datetime.date.today()
    from_date = today
    if 'year' in kwargs:
        from_date = datetime.date(today.year - int(kwargs['year']),
                                  today.month,
                                  today.day)
    elif 'day' in kwargs:
        from_date = today - datetime.timedelta(days=int(kwargs['day']))
    date_diff = today - from_date
    for i in range(0, date_diff.days):
        import1(conn,
                (from_date + datetime.timedelta(days=i)).strftime(datefmt))
    import1(conn, today.strftime(datefmt))


def import_today(conn):
    import_historical(conn)


def create_stock(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
    (symbol text, name text, currency text)''')
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS stocks_symbol_ind
    on stocks(symbol)''')
    conn.commit()
    c.close()


def export1(conn, symb, sname):
    cc = conn.cursor()
    todaystr = datetime.date.today().strftime('%Y-%m-%d')
    cc.execute('SELECT count(*) FROM ' +
               prefix + symb +
               ' WHERE tdate = ?',
               (todaystr,))
    for cnt in cc:
        if cnt[0] == 1:
            with open('data/sgx/' + symb + '.txt', 'w', newline='') as ofile:
                writer = csv.writer(ofile)
                cc.execute('SELECT * FROM ' +
                           prefix + symb +
                           ' ORDER BY tdate DESC')
                #keepcharacters = (' ', '_')
                #ssname = "".join(c for c in sname if c.isalnum() or c in keepcharacters).rstrip()
                for row in cc:
                    print('exp', symb, row)
                    writer.writerow([symb,
                                     datetime.datetime.strptime(row[0], '%Y-%m-%d').strftime('%Y%m%d'),
                                     row[1],
                                     row[2],
                                     row[3],
                                     row[4],
                                     row[5]])
    cc.close()


def export_metastock(conn):
    c = conn.cursor()
    c.execute('SELECT symbol, name FROM stocks')
    for row in c:
        #print(row)
        export1(conn, row[0], row[1])
    c.close()


def main():
    conn = sqlite3.connect('sgx.db')
    create_stock(conn)

    if len(sys.argv) > 1:
        if len(sys.argv) > 2:
            kwargs = {}
            kwargs[sys.argv[1]] = sys.argv[2]
            import_historical(conn, **kwargs)
        elif sys.argv[1] == 'yesterday':
            import_historical(conn, day=1)
        elif sys.argv[1] == 'today':
            import_today(conn)

    export_metastock(conn)

    conn.close()

if __name__ == '__main__':
    main()
