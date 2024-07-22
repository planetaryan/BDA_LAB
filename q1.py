
with open('text.txt','r') as file:
    content_list=file.read().split()
    d={}
    for i in content_list:
        if i in d:
            d[i]+=1
        else:
            d[i]=1

print("No. of words: {}".format(len(content_list)))
sorted_keys_list=sorted(d,key=d.get,reverse=True)

for i in sorted_keys_list:
    print(i+":"+str(d[i]))

