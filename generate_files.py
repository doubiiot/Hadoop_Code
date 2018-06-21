import random
def random_info(cla,count):
    name = ""
    num = "20150106"+("0"+str(cla))[-2:] + ("0"+str(count))[-2:]
    char = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for i in range(random.randint(2,3)):
        name = name + char[random.randint(0,25)]
    score = random.randint(0, 100)
    info = str(num) + " " + str(name) + " " + str(score) + "\n"
    return info

#20为生成文件数
for i in range(1,20):
    info = []
    print("class",i)
    file_name = "./file" + str(i) + ".txt"
    #print(file_name)
    for j in range(i):
        info.append(random_info(i,j+1))
    print(info)
    #for x in range(len(info)):
        #print(info[x][0])
    with open(file_name,'w+') as f:
        for x in range(len(info)):
            f.writelines(info[x])

