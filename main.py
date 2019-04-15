import os

for r,d,f in os.walk('./corpora'):
    print(r,d,f)
    path = r.split('/')
    if len(path) > 2:
        print(path[2])