import argparse
import json
from .collector import collect
p=argparse.ArgumentParser()
p.add_argument('--root',required=True)
a=p.parse_args()
r=collect(a.root)
print(json.dumps({'messages':len(r['messages']),'sources':r['sources']},ensure_ascii=False))
