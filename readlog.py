total = 0
with open('./.data/test.log') as fin:
    i = 0
    for line in fin:
        i += 1
        if i < 3:
            print(i, line)
            continue
        line = line.strip().split()
        bl = int(line[0])
        total += bl

print(total)
