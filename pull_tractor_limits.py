import urllib
import json
from datetime import datetime

TODAY = datetime.today().strftime('%m%d%Y')
TIME_NOW = datetime.now()
TIME = TIME_NOW.strftime('%Y-%m-%d %H:%M:%S')
FILE_DIR = "/mnt/w/PubLogs/render/datadog_tractorlimits_{}.log".format(TODAY)

page = urllib.urlopen("http://tractor-engine/Tractor/queue?q=limits")
web_json = json.loads(page.read())

logfile = open(FILE_DIR, "a")
for limit in web_json["Limits"]:
    if web_json["Limits"][limit]['SiteMax'] != -1:
        tractor_limit = {'timestamp': TIME,
                         'limit': limit,
                         'SiteMax': web_json["Limits"][limit]['SiteMax'],
                         'InUse': web_json["Limits"][limit]["counts"]["site"]}
        json.dumps(tractor_limit)

        logfile.write("{}\n".format(json.dumps(tractor_limit)))
