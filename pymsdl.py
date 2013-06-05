import csv
import urllib.request
import datetime

def get_quotes():
    return [ row for row in csv.reader(open("C:/ms/pymsdl/sgx2.csv"), delimiter='\t') ]

def download_history(quote, name):
    today = datetime.datetime.today()
    yahoo_url = ("http://ichart.finance.yahoo.com/table.csv?s=" +
                 quote +
                 "&a=00&b=1&c=" +
                 str(today.year - 1) +
                 "&d=" +
                 str(today.month - 1).zfill(2) +
                 "&e=" +
                 str(today.day) + 
                 "&f=" +
                 str(today.year) +
                 "&g=d&ignore=.csv")
    try:
        #check whether we need to download
        try:
            reader = csv.reader(open("C:/ms/pymsdl/dldata/sgx/" + name + ".csv")) 
            next(reader) #ignore header line
            row = next(reader)
            if (datetime.datetime.today() - datetime.datetime.strptime(row[0], "%Y-%m-%d")).days <= 1:
                #no need to update
                print("Already updated")
                return 1
        except:
            pass
                                
        # Download the file from `url` and save it locally under `file_name`:
        with open("C:/ms/pymsdl/dldata/sgx/" + name + ".csv", 'wb') as out_file:
            result = urllib.request.urlopen(yahoo_url).read() # a `bytes` object
            out_file.write(result)
        return 0
    except urllib.error.HTTPError:
        return 1

def format_date(d):
    ds = datetime.datetime.strptime(d, "%Y-%m-%d") - datetime.timedelta(days=365)
    return ds.strftime("%Y%m%d")
    
def transform_csv(quote, name):
    writer = csv.writer(open("C:/ms/pymsdl/dldata/sgx/" + name + ".txt", "w"))
    reader = csv.reader(open("C:/ms/pymsdl/dldata/sgx/" + name + ".csv"))
    next(reader) #ignore header line
    for row in reader:            
        writer.writerow([quote,
                         format_date(row[0]),
                         row[1],
                         row[2],
                         row[3],
                         row[4],
                         row[5]])
            

def main():
    for row in get_quotes():        
        quote = row[5] + ".SI"
        keepcharacters = (' ','.','_')
        name = "".join(c for c in row[0] if c.isalnum() or c in keepcharacters).rstrip()
        if not quote.startswith("Ticker"):
            print(quote + "\t" + name)
            if download_history(quote, name) == 0:
                transform_csv(quote, name)

if __name__ == '__main__': 
    main()
