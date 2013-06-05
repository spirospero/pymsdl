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
    try:
        # Download the file from `url` and save it locally under `file_name`:
        out_file = open("dldata/sgx/" + name + ".csv", 'wb')
        result = urllib.request.urlopen(yahoo_url).read()  # a `bytes` object
        out_file.write(result)
        out_file.close()
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
    writer = csv.writer(open("dldata/sgx/" + name + ".hist", "w"))
    reader = csv.reader(open("dldata/sgx/" + name + ".csv"))
    next(reader)  # ignore header line
    for row in reader:
        writer.writerow([quote,
                         format_hist_date(row[0]),
                         row[1],
                         row[2],
                         row[3],
                         row[4],
                         row[5]])
    reader.close()
    writer.close()


def check(quote, name):
    #check whether we need to download
    try:
        reader = csv.reader(open("dldata/sgx/" + name + ".csv"))
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
    try:
        # Download the file from `url` and save it locally under `file_name`:
        out_file = open("dldata/sgx/" + name + ".new", "ab")
        result = urllib.request.urlopen(yahoo_url).read()  # a `bytes` object
        out_file.write(result)
        out_file.close()
        return 0
    except urllib.error.HTTPError:
        return 1


def combine_csv(quote, name):
    writer = csv.writer(open("dldata/sgx/" + name + ".txt", "w"))
    reader = csv.reader(open("dldata/sgx/" + name + ".new"))
    for row in reader:
        writer.writerow([quote,
                         format_new_date(row[1]),
                         row[2],
                         row[3],
                         row[4],
                         row[5],
                         row[6]])
    reader.close()
    reader = csv.reader(open("dldata/sgx/" + name + ".hist"))
    next(reader)  # ignore header line
    for row in reader:
        writer.writerow([quote,
                         format_hist_date(row[0]),
                         row[1],
                         row[2],
                         row[3],
                         row[4],
                         row[5]])


def process(quote, name):
    print(quote + "\t" + name)
    if download_history(quote, name) == 0:
        transform_history_csv(quote, name)
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
