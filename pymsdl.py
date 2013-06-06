import csv
import urllib.request
import datetime


def get_quotes():
    return [row for row in csv.reader(open("sgx2.csv"), delimiter='\t')]


def download_history(quote, name):
    yahoo_url = ("http://ichart.finance.yahoo.com/table.csv?s=" +
                 quote +
                 "&a=00&b=1&c=" +
                 str(2011) +
                 "&d=" +
                 str(6).zfill(2) +
                 "&e=" +
                 str(4) +
                 "&f=" +
                 str(2013) +
                 "&g=d&ignore=.csv")
    print(yahoo_url)
    try:
        # Download the file from `url` and save it locally under `file_name`:
        with open("dldata/sgx/" + name + ".csv", 'wb') as out_file:
            result = urllib.request.urlopen(yahoo_url).read()  # a `bytes` object
            out_file.write(result)
        return 0
    except urllib.error.HTTPError:
        return 1


def format_hist_date(d):
    ds = datetime.datetime.strptime(d, "%Y-%m-%d") - datetime.timedelta(days=365)
    return ds.strftime("%Y%m%d")


def format_new_date(d):
    ds = datetime.datetime.strptime(d, "%m/%d/%Y") - datetime.timedelta(days=365)
    return ds.strftime("%Y%m%d")


def transform_history_csv(quote, name):
    with open("dldata/sgx/" + name + ".hist", "w", newline = '') as wfile:
        writer = csv.writer(wfile)
        with open("dldata/sgx/" + name + ".csv", newline = '') as rfile:
            reader = csv.reader(rfile)
            next(reader)  # ignore header line
            for row in reader:
                writer.writerow([quote,
                                 format_hist_date(row[0]),
                                 row[1],
                                 row[2],
                                 row[3],
                                 row[4],
                                 row[5].rstrip()])


def check(quote, name):
    #check whether we need to download
    try:
        reader = csv.reader(open("dldata/sgx/" + name + ".csv", newline = ''))
        next(reader)  # ignore header line
        row = next(reader)
        if (datetime.datetime.today() - datetime.datetime.strptime(row[0], "%Y-%m-%d")).days <= 1:
            #no need to update
            print("Already updated")
            return 1
        reader.close()
    except:
        pass



def get_latest(quote, name):
    yahoo_url = ("http://download.finance.yahoo.com/d/quotes.csv?s=" +
                 quote +
                 "&f=sd1o0h0g0l1v0p0")
    #try:
        # Download the file from `url` and save it locally under `file_name`:
    with open("dldata/sgx/" + name + ".new", "ab") as out_file:
        result = urllib.request.urlopen(yahoo_url).read()  # a `bytes` object
        out_file.write(result)
    return 0
    #except urllib.error.HTTPError:
    #    return 1


def combine_csv(quote, name):
    try:
        with open("dldata/sgx/" + name + ".txt", "w", newline = '') as wfile:
            writer = csv.writer(wfile)
            with open("dldata/sgx/" + name + ".new", newline = '') as rfile:
                reader = csv.reader(rfile)
                for row in reader:
                    if row[2] == "N/A":
                        return
                    writer.writerow([quote,
                                     format_new_date(row[1]),
                                     row[2],
                                     row[3],
                                     row[4],
                                     row[5],
                                     row[6]])
            with open("dldata/sgx/" + name + ".hist", newline = '') as rfile:
                reader = csv.reader(rfile)
                #next(reader)  # ignore header line
                for row in reader:
                    writer.writerow(row)
    except:
        pass

def process(quote, name):
    print(quote + "\t" + name)
    if download_history(quote, name) == 0:
        transform_history_csv(quote, name)
    #download_history(quote, name)
    get_latest(quote, name)
    combine_csv(quote, name)


def main():
    for row in get_quotes():
        quote = row[5] + ".SI"
        keepcharacters = (' ', '.', '_')
        name = "".join(c for c in row[0] if c.isalnum() or c in keepcharacters).rstrip()
        if not quote.startswith("Ticker"):
            process(quote, name)


if __name__ == '__main__':
    main()
