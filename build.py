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

insert_cursor = db.cursor()

punctuation_mapping = {
    'q' : '?',
    'e' : '!',
    'trail off':'.',
    'p': '.'
}

def get_node_name(node):
    matchs = re.findall('[a-zA-Z]+(?![^{]*\})',node)
    if len(matchs) > 0:
        return matchs[0]
    return None

def get_participant(id):
    p_cursor = db.cursor(prepared=True)

    p_cursor.execute("SELECT * FROM participant WHERE id = %s",(id,))

    return p_cursor.fetchone()

def insert(entity, data):
    sql = "INSERT INTO "+entity+" ("+(', '.join(data.keys()))+") VALUES ("+(', '.join('%s' for a in range(len(data.keys()))))+");"
    values = tuple(data.values())

    # print(sql)
    # print(values)
    i_cursor = db.cursor(prepared=True)
    i_cursor.execute(sql,values)

    db.commit()


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
                                
                                speaker = get_participant(utterance['speaker_id'])

                                target_child_id = None
                                target_child_age = None
                                target_child_name = None
                                target_child_sex = None

                                if speaker[13] != None:
                                    tc = get_participant(speaker[13])
                                    target_child_id = speaker[13]
                                    target_child_age = tc[12]
                                    target_child_name = tc[2]
                                    target_child_sex = tc[6]
                                

                                sql = "insert into utterance_jap (speaker_id,`order`, corpus_id, speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex, target_child_age) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                # print(sql)
                                insert_cursor.execute(sql, (utterance['speaker_id'],utterance['order'],utterance['corpus_id'],speaker[12],speaker[1],   speaker[2],     speaker[3],  speaker[6],  speaker[13],     target_child_name, target_child_age, target_child_age ))
                                # "insert into utterance_jap (speaker_id,             `order`,            corpus_id,            speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex, target_child_age) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                db.commit()

                                utterance_id = insert_cursor.lastrowid
                                i = 0
                                for token in child:
                                    if get_node_name(token.tag) in ['w','t','tagMarker']:
                                        tk = {
                                            'speaker_id':speaker[0],
                                            'utterance_id': utterance_id,
                                            'token_order':i,
                                            'corpus_id':corpus['id']
                                        }
                                        if get_node_name(token.tag) == 'w':
                                            # WORD
                                            # print(token.text)
                                            tk['gloss'] = token.text
                                            replacement = token.find(ns+'replacement')
                                            if replacement != None:
                                                tk['replacement'] = replacement.text
                                                token = replacement

                                            if token.find(ns+'mor') != None:
                                                if token.find(ns+'mor').find(ns+'mwc') != None:
                                                    tk['part_of_speech'] = token.find(ns+'mor').find(ns+'mwc').find(ns+'pos').find(ns+'c').text
                                                    stems = []
                                                    for mw in token.find(ns+'mor').find(ns+'mwc').findall(ns+'mw'):
                                                        stems.append(mw.find(ns+"stem").text)
                                                    tk['stem'] = ''.join(stems)
                                                else:
                                                    tk['part_of_speech'] = token.find(ns+'mor').find(ns+'mw').find(ns+'pos').find(ns+'c').text
                                                    tk['stem'] = token.find(ns+'mor').find(ns+'mw').find(ns+'stem').text

                                                gra = token.find(ns+'mor').find(ns+'gra')
                                                if gra != None:
                                                    tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']
                                            
                                            if token.find(ns+"shortening") != None:
                                                tk['gloss']  = tk['stem']
                                            
                                            
                                        elif get_node_name(token.tag) == 't':
                                            #punctuation
                                            _type = token.attrib['type']
                                            if _type in punctuation_mapping:
                                                tk['gloss'] = punctuation_mapping[_type]
                                                tk['stem'] = punctuation_mapping[_type]
                                                tk['part_of_speech'] = "f"
                                                
                                                if token.find(ns+'mor') != None:
                                                    gra = token.find(ns+'mor').find(ns+'gra')
                                                    if gra != None:
                                                        tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']

                                        elif get_node_name(token.tag) == 'tagMarker':
                                            tk['gloss'] = ','
                                            tk['stem'] = ','
                                            tk['part_of_speech'] = "f"
                                            
                                            if token.find(ns+'mor') != None:
                                                gra = token.find(ns+'mor').find(ns+'gra')
                                                if gra != None:
                                                    tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']
                                            # comma, etc
                                            pass
                                        i = i +1
                                        print(tk)
                                        insert('token_jap',tk)
                                        del tk
                                print('\n')

                                