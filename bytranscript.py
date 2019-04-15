import mysql.connector 
import language_check
import json
import os
import xml.etree.ElementTree as ET
import re

f = open('config.json','r')

config = json.loads(f.read())

db = mysql.connector.connect(
    host=config['host'],
    user=config['user'],
    passwd=config['passwd'],
    database=config['database']
)

ns = "{http://www.talkbank.org/ns/talkbank}"

cursor = db.cursor()
cursor.execute("select B.id, B.filename from transcript_by_speaker A inner join transcript B ON A.transcript_id = B.id where A.speaker_role = 'Target_Child' and A.mlu > 2.25 and A.target_child_age/30 between 30 and 144;")

transcripts = cursor.fetchall()

for transcript in transcripts:
    if not os.path.isfile('./corpora/'+transcript[1]):
        print(transcript)
