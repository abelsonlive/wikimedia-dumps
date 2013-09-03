from thready import threaded
import dataset
import requests
import gzip
from StringIO import StringIO
import re
from datetime import datetime
from gevent.pool import Pool 

def url_to_date(url):
  d = "".join(url.split("/")[-1].split(".")[0].split("-")[1:3])
  return datetime.strptime(d, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

def url_to_fp(url):
  return "data/" + "".join(url.split("/")[-1].split(".")[0].split("-")[1:3]) + "-en.tsv"

def decode_gzip(gzipped_string):
    ''' decode gzipped content '''
    gzipper = gzip.GzipFile(fileobj=StringIO(gzipped_string))
    return gzipper.read()

def get_data(url):
  print "downloading %s" % url
  dt = url_to_date(url)
  r = requests.get(url)
  if r.status_code != 200:
    print r.status_code
    print r.content
    print "trouble reading %s" % url
  else:
    print "decoding %s" % url
    tsv = decode_gzip(r.content)
    data = []
    print "filtering %s" % url
    for line in tsv.split("\n"):
      if re.match("en ", line):
        line = unicode(line, errors="ignore")
        fields = line.split(" ")
        row = "\t".join([fields[1].strip(), fields[2].strip(), dt])
        data.append(row)

    print "writing %d rows to file" % len(data)
    string = "\n".join(data)
    f = open(url_to_fp(url), "w")
    f.write(string)

if __name__ == '__main__':
  p = Pool(8)
  urls = [u for u in open('election-dumps.txt').read().split("\n") if u is not ""]
  p.map(get_data, urls)

