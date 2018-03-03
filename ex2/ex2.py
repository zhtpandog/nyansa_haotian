from functools import reduce
import sys

if __name__ == "__main__":
    def map_phase_1(record):
        id, device_type, score = record.rstrip('}').split(" = ")[1].lstrip("{").replace('''"''', "").replace(" ", "").split(",")
        return id, device_type, int(score), 1


    def reduce_phase_1(record1, record2):
        return record1[0], record1[1] + record2[1], record1[2] + record2[2]


    def map_phase_2(record):
        id = record[0]
        device_type, total, count = record[1][0], record[1][1], record[1][2]
        return device_type, True if total / count <= 50 else False, 0, 0

    def reduce_phase_2(record1, record2):
        poor1, count11, count12 = record1[0], record1[1], record1[2]
        poor2, count21, count22 = record2[0], record2[1], record2[2]
        if count11 == 0:
            count21 = 2
            count22 = poor1 + poor2
        else:
            count21 += 1
            count22 += poor2
        return poor2, count21, count22


    def map_phase_3(record):
        return record[0], record[1][2] / record[1][1]

    if len(sys.argv) != 2:
        print ("Please offer input file name and put it in the same folder as this script.")
        exit(1)

    file_name = sys.argv[1]

    # map 1
    with open(file_name) as f:
        input_str = f.read().split("\n")

    print ("file loaded")

    mapped = map(map_phase_1, input_str)

    print ("phase 1 map finished")

    # group by key (shuffling) 1
    group = {}
    for id, device_type, score, count in mapped:
        if id not in group:
            group[id] = [[device_type, score, count]]
        else:
            group[id].append([device_type, score, count])

    # reduce 1
    for id in group:
        group[id] = reduce(reduce_phase_1, group[id])

    print ("phase 1 reduce finished")

    # map 2
    mapped2 = map(map_phase_2, group.items())

    print ("phase 2 map finished")

    # group by key (shuffling) 2
    group2 = {}
    for device_type, poor, count1, count2 in mapped2:
        if device_type not in group2:
            group2[device_type] = [[poor, count1, count2]]
        else:
            group2[device_type].append([poor, count1, count2])

    # reduce 2
    for device_type in group2:
        group2[device_type] = reduce(reduce_phase_2, group2[device_type])

    print ("phase 2 reduce finished")

    # map 3
    mapped3 = list(map(map_phase_3, group2.items()))

    print ("phase 3 map finished")
    print ("result: ")

    # produce result
    mapped3.sort(key=lambda x: x[1], reverse=True)

    highest_ratio = mapped3[0][1]
    for i in mapped3:
        if i[1] == highest_ratio:
            print (i[0])
        else:
            break