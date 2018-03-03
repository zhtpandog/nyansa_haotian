import sys
from pyspark import SparkContext

def map_phase_1(record):
    id, device_type, score = record.rstrip('}').split(" = ")[1].lstrip("{").replace('''"''', "").replace(" ", "").split(",")
    return id, (device_type, int(score))

def map_phase_2(record):
    id = record[0]
    device_type, total, count = record[1][2], record[1][0], record[1][1]
    return device_type, True if total / count <= 50 else False

def map_phase_3(record):
    return record[0], float(record[1][1]) / record[1][0]

sc = SparkContext(appName="nyansa_haotian")

source = sc.textFile("input.txt")

phase1_rdd = source.map(map_phase_1).aggregateByKey((0, 0, None), lambda u,v: (u[0] + v[1], u[1] + 1, v[0]), lambda u1, u2: (u1[0] + u2[0], u1[1] + u2[1], v[0]))
phase2_rdd = phase1_rdd.map(map_phase_2).aggregateByKey((0, 0), lambda u, v: (u[0] + 1, u[1] + v), lambda u1, u2: (u1[0] + u2[0], u1[1] + u2[1], v[0]))
result = phase2_rdd.map(map_phase_3).collect()
result.sort(key=lambda x: x[1], reverse=True)

highest_ratio = result[0][1]
for i in result:
    if i[1] == highest_ratio:
        print (i[0])
    else:
        break



















