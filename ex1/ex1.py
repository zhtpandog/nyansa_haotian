import time
import sys

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Please offer input file name and put it in the same folder as this script.")
        exit(1)

    file_name = sys.argv[1]

    # compose dict
    ts_n_web_count = {}
    with open(file_name) as f:
        for line in f:
            ts, web = line.split("|")
            ts = time.strftime("%Y%m%d", time.gmtime(int(ts)))
            web = web.rstrip('\n')
            if ts not in ts_n_web_count:
                ts_n_web_count[ts] = {web: 1}
            else:
                if web not in ts_n_web_count[ts]:
                    ts_n_web_count[ts][web] = 1
                else:
                    ts_n_web_count[ts][web] += 1

    # sort within each ts
    for ts in ts_n_web_count:
        ts_n_web_count[ts] = sorted(ts_n_web_count[ts].items(), key=lambda x: x[1], reverse=True)

    # sort ts
    ts_n_web_count = sorted(ts_n_web_count.items(), key=lambda x: x[0])

    # print result
    for ts, web_list in ts_n_web_count:
        print(time.strftime("%m/%d/%Y", time.strptime(ts, "%Y%m%d")) + " " + "GMT")
        for web, count in web_list:
            print(web + " " + str(count))


