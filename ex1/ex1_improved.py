import time
import sys

# first pass get max hit per day per URL, and construct dict {day:{URL:times}}
# second pass initiate {day:{times:[URL_ID_list]}} via bucket sort
# sort date, report data
# time: O(N) + O(M) + O(KlogK)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Please offer input file name and put it in the same folder as this script.")
        exit(1)

    file_name = sys.argv[1]

    # compose dict {day:{URL_ID:times}}
    ts_n_url_count = {}
    ts_n_count_url = {}
    max_hit = 1
    with open(file_name) as f:
        for line in f:
            ts, url = line.split("|")
            ts = time.strftime("%Y%m%d", time.gmtime(int(ts)))
            url = url.rstrip('\n')
            if ts not in ts_n_count_url:
                ts_n_count_url[ts] = []
            if ts not in ts_n_url_count:
                ts_n_url_count[ts] = {url: 1}
            else:
                if url not in ts_n_url_count[ts]:
                    ts_n_url_count[ts][url] = 1
                else:
                    ts_n_url_count[ts][url] += 1
                    max_hit = max(max_hit, ts_n_url_count[ts][url])

    # compose dict {day:[URL_ID_list]}
    for ts in ts_n_url_count:
        for url in ts_n_url_count[ts]:
            count = ts_n_url_count[ts][url]
            if not ts_n_count_url[ts]:
                ts_n_count_url[ts] = [[] for _ in range(max_hit + 1)]
            ts_n_count_url[ts][count].append(url)

    # compose dict {day: [[URL: count]]}
    for ts in ts_n_count_url:
        for times in range(len(ts_n_count_url[ts]) - 1, 0, -1):
            urls = ts_n_count_url[ts][times]
            if not urls:
                continue
            if type(ts_n_url_count[ts]) == dict:
                ts_n_url_count[ts] = []
            for url in urls:
                ts_n_url_count[ts].append([url, times])

    # sort by ts
    ts_n_url_count = sorted(ts_n_url_count.items(), key=lambda x: x[0])

    # print result
    for ts, url_list in ts_n_url_count:
        print(time.strftime("%m/%d/%Y", time.strptime(ts, "%Y%m%d")) + " " + "GMT")
        for url, count in url_list:
            print(url + " " + str(count))


