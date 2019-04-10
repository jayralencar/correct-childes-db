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

insert_cursor = db.cursor()

def get_node_name(node):
    matchs = re.findall('[a-zA-Z]+(?![^{]*\})',node)
    if len(matchs) > 0:
        return matchs[0]
    return None


for r,d,f in os.walk('./corpora'):
    if len(d) > 0:
        for dirname in d:
            corpus = {}
            corpus_cursor = db.cursor(prepared=True)
            corpus_cursor.execute("SELECT * FROM corpus where name = %s",(dirname,))

            corpora = corpus_cursor.fetchall()

            if len(corpora) > 0:
                corpus = {
                    'id' : corpora[0][0],
                    'name' : corpora[0][1].decode("utf-8")
                }
                
                for r,d,f in os.walk('./corpora/'+dirname):
                    for filename in f[:1]:
                        print(filename)
                        tree = ET.parse('./corpora/'+dirname+"/"+filename)
                        root = tree.getroot()
                        participants = {}
                        for child in root:
                            # Participants
                            if get_node_name(child.tag) =="Participants":
                                for participant in child:
                                    participant_cursor = db.cursor(prepared=True)
                                    if 'name' in participant.attrib:
                                        participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND name = %s AND code = %s AND role = %s", (corpus['id'],participant.attrib['name'],participant.attrib['id'],participant.attrib['role'], ))
                                        ps = participant_cursor.fetchall()
                                        participants[participant.attrib['id']] = ps[0][0]
                                    elif 'CHI' in participants:
                                        participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND target_child_id = %s AND code = %s AND role = %s", (corpus['id'],participants['CHI'],participant.attrib['id'],participant.attrib['role'], ))
                                        ps = participant_cursor.fetchall()
                                        participants[participant.attrib['id']] = ps[0][0]
                            
                            if get_node_name(child.tag) =="u":
                                utterance = {
                                    "speaker_id": participants[child.attrib['who']],
                                    'order': int(child.attrib['uID'].split('u')[1]),
                                    'corpus_id': corpus['id']
                                }

                                speaker_information_cursor = db.cursor(prepared=True)

                                speaker_information_cursor.execute("SELECT * FROM participant WHERE id = %s",(utterance['speaker_id'],))

                                speaker = speaker_information_cursor.fetchone()

                                sql = "insert into utterance_jap (speaker_id,`order`, corpus_id, speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

                                insert_cursor.execute(sql, (utterance['speaker_id'],utterance['order'],utterance['corpus_id'],speaker[12],speaker[1],speaker[2],speaker[3],speaker[6],speaker[13]))

                                "insert into utterance_jap (speaker_id,`order`, corpus_id, speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                