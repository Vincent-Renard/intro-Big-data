
from os import listdir, sep

path = "data/semaine2-fichiers/input-word-count"
files=[]
for filetxt in listdir(path=path):
    with open(path+sep+filetxt,'r') as f :

        files.extend( line[:-1] for line in f.readlines() )

words=[]
for line in files:
    ws = line.split(' ')
    for word in ws:
        words.append(word.lower())


words = ["".join(c for c in w if c.isalpha()) for w in words]
wc={w:0 for w in words}

for w in words :
    wc[w]+=1
for w,c in wc.items():
    print(w,'\t',c)


